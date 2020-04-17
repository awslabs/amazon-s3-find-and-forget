import { apiGateway, glueGateway, stsGateway } from "./request";

export default {
  deleteDataMapper(id) {
    return apiGateway(`data_mappers/${id}`, { method: "del" });
  },

  deleteQueueMatches(matches) {
    return apiGateway(`queue/matches`, {
      method: "del",
      data: {
        Matches: matches
      }
    });
  },

  enqueue(id, dataMappers) {
    return apiGateway(`queue`, {
      method: "patch",
      data: { MatchId: id, DataMappers: dataMappers }
    });
  },

  getAccountId() {
    return stsGateway("/?Action=GetCallerIdentity&Version=2011-06-15");
  },

  getDataMappers() {
    return apiGateway("data_mappers");
  },

  async getGlueDatabases() {
    const all = [];
    let watermark = undefined;
    while (true) {
      const body = watermark ? { NextToken: watermark } : {};
      const page = await glueGateway("GetDatabases", body);
      all.push(...page.DatabaseList);

      if (page.NextToken) watermark = page.NextToken;
      else break;
    }
    return { DatabaseList: all };
  },

  getGlueTable(database, table) {
    return glueGateway("GetTable", {
      DatabaseName: database,
      Name: table
    });
  },

  async getGlueTables(database) {
    const all = [];
    let watermark = undefined;
    while (true) {
      const body = Object.assign(watermark ? { NextToken: watermark } : {}, {
        DatabaseName: database
      });
      const page = await glueGateway("GetTables", body);
      all.push(...page.TableList);

      if (page.NextToken) watermark = page.NextToken;
      else break;
    }
    return { TableList: all };
  },

  getJob(jobId) {
    return apiGateway(`jobs/${jobId}`);
  },

  getLastJob() {
    return apiGateway(`jobs?page_size=1`);
  },

  async getJobs() {
    const allJobs = [];
    let watermark = undefined;

    while (true) {
      const qs = watermark ? `?start_at=${watermark}` : "";
      const page = await apiGateway(`jobs${qs}`);
      allJobs.push(...page.Jobs);

      if (page.NextStart) watermark = page.NextStart;
      else break;
    }
    return { Jobs: allJobs };
  },

  async getJobEvents(
    jobId,
    watermark = undefined,
    page_size = 20,
    filters = []
  ) {
    let qs = `?page_size=${page_size}`;
    if (watermark) {
      qs += `&start_at=${encodeURIComponent(watermark)}`;
    }
    if (filters.length > 0) {
      qs +=
        "&" +
        filters
          .map(
            f => `filter=${encodeURIComponent(f.key + f.operator + f.value)}`
          )
          .join("&");
    }
    return await apiGateway(`jobs/${jobId}/events${qs}`);
  },

  getQueue() {
    return apiGateway("queue");
  },

  getSettings() {
    return apiGateway("settings");
  },

  processQueue() {
    return apiGateway("queue", { method: "del" });
  },

  putDataMapper(id, db, table, columns, roleArn, deleteOldVersions) {
    return apiGateway(`data_mappers/${id}`, {
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
        Format: "parquet",
        RoleArn: roleArn,
        DeleteOldVersions: deleteOldVersions
      }
    });
  }
};
