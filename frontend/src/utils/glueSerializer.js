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
