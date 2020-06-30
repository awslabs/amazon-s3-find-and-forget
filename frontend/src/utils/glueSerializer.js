export const glueSerializer = tables => {
  const result = { databases: [] };

  const PARQUET_SERDE =
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
  const JSON_SERDE = "org.openx.data.jsonserde.JsonSerDe";

  tables.forEach(table => {
    const parquetTables = table.TableList.filter(x =>
      [PARQUET_SERDE, JSON_SERDE].includes(
        x.StorageDescriptor.SerdeInfo.SerializationLibrary
      )
    );

    if (parquetTables.length > 0)
      result.databases.push({
        name: parquetTables[0].DatabaseName,
        tables: parquetTables.map(t => ({
          name: t.Name,
          columns: t.StorageDescriptor.Columns.map(x => x.Name),
          format:
            t.StorageDescriptor.SerdeInfo.SerializationLibrary === JSON_SERDE
              ? "json"
              : "parquet"
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
