import request from "./request";

export default {
  deleteDataMapper(id) {
    return request(`data_mappers/${id}`, { method: "del" });
  },

  getDataMappers() {
    return request("data_mappers");
  },

  putDataMapper(id, db, table, columns) {
    return request(`data_mappers/${id}`, {
      method: "put",
      data: {
        DataMapperId: id,
        Columns: columns,
        QueryExecutor: "athena",
        QueryExecutorParameters: {
          DataCatalogProvider: "glue",
          Database: db,
          Table: table
        },
        Format: "parquet"
      }
    });
  }
};
