import request from "./request";

export default {
  deleteDataMapper(id) {
    return request(`data_mappers/${id}`, { method: "del" });
  },

  deleteQueueMatch(id) {
    return request(`queue/matches/${id}`, { method: "del" });
  },

  enqueue(id, dataMappers) {
    return request(`queue`, {
      method: "patch",
      data: { MatchId: id, DataMappers: dataMappers }
    });
  },

  getDataMappers() {
    return request("data_mappers");
  },

  getQueue() {
    return request("queue");
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
