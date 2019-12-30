import request from "./request";

import { isEmpty } from "./";

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

  getJob(jobId) {
    return request(`jobs/${jobId}`);
  },

  getJobs(pageSize, startAt) {
    let qs = "?";
    if (!isEmpty(pageSize)) qs += `page_size=${pageSize}&`;
    if (!isEmpty(startAt)) qs += `start_at=${startAt}&`;

    return request(`jobs${qs.slice(0, -1)}`);
  },

  getQueue() {
    return request("queue");
  },

  processQueue() {
    return request("queue", { method: "del" });
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
