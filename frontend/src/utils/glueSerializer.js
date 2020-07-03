export const glueSerializer = tables => {
  const result = { databases: [] };

  const ARRAYSTRUCTPREFIX = "array<struct<";
  const STRUCTPREFIX = "struct<";
  const SCHEMA_INVALID = "Column schema is not valid";

  const columnMapper = c => {
    let prefix,
      suffixLength,
      resultType = c.Type;

    if (c.Type.startsWith(ARRAYSTRUCTPREFIX)) {
      resultType = "array<struct>";
      prefix = ARRAYSTRUCTPREFIX;
      suffixLength = 2;
    } else if (c.Type.startsWith(STRUCTPREFIX)) {
      resultType = "struct";
      prefix = STRUCTPREFIX;
      suffixLength = 1;
    }

    const result = { name: c.Name, type: resultType };

    if (prefix) {
      if (c.Type.slice(-suffixLength) !== ">".repeat(suffixLength))
        throw new Error(SCHEMA_INVALID);

      result.children = [];
      let current = c.Type.substr(prefix.length).slice(0, -suffixLength);

      while (true) {
        if (current.length === 0) break;
        const name = current.substr(0, current.indexOf(":"));
        const rest = current.substr(name.length + 1);
        const isStruct = rest.startsWith(STRUCTPREFIX);
        const isStructArray = rest.startsWith(ARRAYSTRUCTPREFIX);

        if (isStruct || isStructArray) {
          const prefix = isStruct ? STRUCTPREFIX : ARRAYSTRUCTPREFIX;
          let nOpenedTags = isStruct ? 1 : 2;
          let endIndex = -1;
          const structSchema = [...rest.substr(prefix.length)];
          structSchema.some((c, i) => {
            if (c === "<") nOpenedTags++;
            if (c === ">") nOpenedTags--;
            if (nOpenedTags === 0) {
              endIndex = i;
              return true;
            }
            return false;
          });

          if (endIndex < 0) throw new Error(SCHEMA_INVALID);

          const type = rest.substr(0, endIndex + prefix.length + 1);
          result.children.push(columnMapper({ Name: name, Type: type }));
          current = current.substr(name.length + type.length + 1);
        } else {
          const upperIndex = rest.indexOf(",");
          const type = upperIndex >= 0 ? rest.substr(0, upperIndex) : rest;
          result.children.push({ name, type });
          current = current.substr(name.length + type.length + 1);
        }
        if (current.startsWith(",")) current = current.substr(1);
      }
    }

    return result;
  };

  tables.forEach(table => {
    const parquetTables = table.TableList.filter(
      x =>
        x.StorageDescriptor.SerdeInfo.SerializationLibrary ===
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    );

    if (parquetTables.length > 0)
      result.databases.push({
        name: parquetTables[0].DatabaseName,
        tables: parquetTables.map(t => ({
          name: t.Name,
          columns: t.StorageDescriptor.Columns.map(columnMapper)
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
