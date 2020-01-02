export const glueSerializer = tables => {
  const result = { databases: [] };

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
          columns: t.StorageDescriptor.Columns.map(x => x.Name)
        }))
      });
  });
  return result;
};

export const bucketMapper = tables => {
  const result = {};

  tables.forEach(
    t =>
      (result[
        `${t.Table.DatabaseName}/${t.Table.Name}`
      ] = t.Table.StorageDescriptor.Location.split("/")[2])
  );

  return result;
};
