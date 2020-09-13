import { isUndefined } from "./";

export const glueSerializer = tables => {
  const result = { databases: [] };

  const PARQUET_SERDE =
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
  const JSON_OPENX_SERDE = "org.openx.data.jsonserde.JsonSerDe";
  const JSON_HIVE_SERDE = "org.apache.hive.hcatalog.data.JsonSerDe";

  const ARRAYSTRUCT = "array<struct>";
  const ARRAYSTRUCTPREFIX = "array<struct<";
  const ARRAYSTRUCTSUFFIX = ">>";

  const STRUCT = "struct";
  const STRUCTPREFIX = "struct<";
  const STRUCTSUFFIX = ">";

  const SCHEMA_INVALID = "Column schema is not valid";
  const ALLOWEDTYPES = [
    "bigint",
    "char",
    "double",
    "float",
    "int",
    "smallint",
    "string",
    "tinyint",
    "varchar"
  ];

  // Function to get inner children from complex type string
  // "struct<name:string,age:int>" => "name:string,age:int"
  const getInnerChildren = (str, prefix, suffix) => {
    if (!str.endsWith(suffix)) throw new Error(SCHEMA_INVALID);
    return str.substr(prefix.length).slice(0, -suffix.length);
  };

  // Function to get next nested child type from a children string
  // starting with a complex type such as struct or array
  // "struct<name:string,age:int,s:struct<n:int>>,b:string" =>
  // "struct<name:string,age:int,s:struct<n:int>>"
  const getNestedChildren = (str, nestedType) => {
    const isStruct = nestedType === STRUCT;
    const prefix = isStruct ? STRUCTPREFIX : ARRAYSTRUCTPREFIX;
    const suffix = isStruct ? STRUCTSUFFIX : ARRAYSTRUCTSUFFIX;
    let nOpenedTags = suffix.length;
    let endIndex = -1;

    const structSchema = [...str.substr(prefix.length)];
    for (let i = 0; i < structSchema.length; i++) {
      const c = structSchema[i];
      if (c === "<") nOpenedTags++;
      if (c === ">") nOpenedTags--;
      if (nOpenedTags === 0) {
        endIndex = i;
        break;
      }
    }

    if (endIndex < 0) throw new Error(SCHEMA_INVALID);
    return str.substr(0, endIndex + prefix.length + 1);
  };

  // Function to get next nested child type from a children string
  // starting with a non complex type
  // "string,a:int" => "string"
  const getNestedType = str => {
    const upperIndex = str.indexOf(",");
    return upperIndex >= 0 ? str.substr(0, upperIndex) : str;
  };

  // Function to set canBeIdentifier=false to item and its children
  // Example:
  // {
  //   name: "arr",
  //   type: "array<struct>",
  //   canBeIdentifier: false,
  //   children: [
  //     { name: "field", type: "int", canBeIdentifier: true },
  //     { name: "n", type: "string", canBeIdentifier: true }
  //   ]
  // }
  // =>
  // {
  //   name: "arr",
  //   type: "array<struct>",
  //   canBeIdentifier: false,
  //   children: [
  //     { name: "field", type: "int", canBeIdentifier: false },
  //     { name: "n", type: "string", canBeIdentifier: false }
  //   ]
  // }
  const setNoIdentifierToNodeAndItsChildren = node => {
    node.canBeIdentifier = false;
    if (node.children)
      node.children.forEach(c => setNoIdentifierToNodeAndItsChildren(c));
  };

  // Function to map Columns from AWS Glue schema to tree
  // Example 1:
  // { Name: "Name", Type: "int" } =>
  // { name: "Name", type: "int", canBeIdentifier: true }
  // Example 2:
  // { Name: "complex", Type: "struct<a:string,b:struct<c:int>>"} =>
  // { name: "complex", type: "struct", children: [
  //    { name: "a", type: "string", canBeIdentifier: false},
  //    { name: "b", type: "struct", children: [
  //      { name: "c", type: "int", canBeIdentifier: false}
  //    ], canBeIdentifier: false}
  // ], canBeIdentifier: false}
  const columnMapper = c => {
    let prefix, suffix;
    let resultType = c.Type;
    let hasChildren = false;

    if (c.Type.startsWith(ARRAYSTRUCTPREFIX)) {
      resultType = ARRAYSTRUCT;
      prefix = ARRAYSTRUCTPREFIX;
      suffix = ARRAYSTRUCTSUFFIX;
      hasChildren = true;
    } else if (c.Type.startsWith(STRUCTPREFIX)) {
      resultType = STRUCT;
      prefix = STRUCTPREFIX;
      suffix = STRUCTSUFFIX;
      hasChildren = true;
    }

    const result = {
      name: c.Name,
      type: resultType,
      canBeIdentifier: !isUndefined(c.canBeIdentifier)
        ? c.canBeIdentifier
        : ALLOWEDTYPES.includes(resultType)
    };

    if (hasChildren) {
      result.children = [];
      let childrenToParse = getInnerChildren(c.Type, prefix, suffix);

      while (childrenToParse.length > 0) {
        const sep = ":";
        const name = childrenToParse.substr(0, childrenToParse.indexOf(sep));
        const rest = childrenToParse.substr(name.length + sep.length);
        const nestedType = rest.startsWith(STRUCTPREFIX)
          ? STRUCT
          : rest.startsWith(ARRAYSTRUCTPREFIX)
          ? ARRAYSTRUCT
          : "other";

        const type =
          nestedType === "other"
            ? getNestedType(rest)
            : getNestedChildren(rest, nestedType);

        result.children.push(
          columnMapper({
            Name: name,
            Type: type,
            canBeIdentifier: ALLOWEDTYPES.includes(type)
          })
        );
        childrenToParse = childrenToParse.substr(
          name.length + sep.length + type.length
        );
        if (childrenToParse.startsWith(","))
          childrenToParse = childrenToParse.substr(1);
      }
      if (resultType !== STRUCT) setNoIdentifierToNodeAndItsChildren(result);
    }

    return result;
  };

  tables.forEach(table => {
    const supportedTables = table.TableList.filter(x =>
      [JSON_HIVE_SERDE, JSON_OPENX_SERDE, PARQUET_SERDE].includes(
        x.StorageDescriptor.SerdeInfo.SerializationLibrary
      )
    );

    if (supportedTables.length > 0)
      result.databases.push({
        name: supportedTables[0].DatabaseName,
        tables: supportedTables.map(t => ({
          name: t.Name,
          columns: t.StorageDescriptor.Columns.map(columnMapper),
          format:
            t.StorageDescriptor.SerdeInfo.SerializationLibrary === PARQUET_SERDE
              ? "parquet"
              : "json"
        }))
      });
  });
  return result;
};

export const bucketMapper = tables => {
  const result = {};

  tables.forEach(
    t =>
      (result[`${t.Table.DatabaseName}/${t.Table.Name}`] = {
        bucket: t.Table.StorageDescriptor.Location.split("/")[2],
        location: t.Table.StorageDescriptor.Location
      })
  );

  return result;
};
