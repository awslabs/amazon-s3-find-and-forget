import { apiGateway, glueGateway, stsGateway } from "./request";

const getPaginatedList = async (endpoint, key, pageSize) => {
  const all = [];
  let watermark = undefined;

  while (true) {
    let qs = `?page_size=${pageSize || 10}`;
    if (watermark) qs += `&start_at=${watermark}`;
    const page = await apiGateway(`${endpoint}${qs}`);
    all.push(...page[key]);

    if (page.NextStart) watermark = encodeURIComponent(page.NextStart);
    else break;
  }
  return all;
};

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
    return { Jobs: await getPaginatedList("jobs", "Jobs") };
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

  async getAllJobEvents(jobId) {
    let watermark = "0";
    let data = [];
    while (watermark) {
      let jobEventsList = await this.getJobEvents(jobId, watermark, 1000);
      data = data.concat(jobEventsList.JobEvents);
      watermark = jobEventsList.NextStart;
    }

    return data;
  },

  async getQueue() {
    return { MatchIds: await getPaginatedList("queue", "MatchIds", 500) };
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
