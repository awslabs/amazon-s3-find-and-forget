import { apiGateway, glueGateway, stsGateway } from "./request";

const getPaginatedList = async (endpoint, key, pageSize) => {
  const all = [];
  let watermark = undefined;

  while (true) {
    let qs = `?page_size=${pageSize || 10}`;
    if (watermark) qs += `&start_at=${watermark}`;
    const page = await apiGateway(`${endpoint}${qs}`, { response: true });
    all.push(...page.data[key]);

    if (page.data.NextStart)
      watermark = encodeURIComponent(page.data.NextStart);
    else break;
  }

  return { response: all };
};

const gateway = {
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

  enqueueSimple(id, dataMappers) {
    return apiGateway(`queue/matches`, {
      method: "patch",
      data: {
        Matches: [{ Type: "Simple", MatchId: id, DataMappers: dataMappers }]
      }
    });
  },

  enqueueComposite(id, dataMapper) {
    return apiGateway(`queue/matches`, {
      method: "patch",
      data: {
        Matches: [
          {
            Type: "Composite",
            MatchId: Object.keys(id).map(column => ({
              Column: column,
              Value: id[column]
            })),
            DataMappers: [dataMapper]
          }
        ]
      }
    });
  },

  getAccountId() {
    return stsGateway("/?Action=GetCallerIdentity&Version=2011-06-15");
  },

  async getDataMappers() {
    const dataMappers = await getPaginatedList(
      "data_mappers",
      "DataMappers",
      100
    );
    return { DataMappers: dataMappers.response };
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
    const jobs = await getPaginatedList("jobs", "Jobs");
    return { Jobs: jobs.response };
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
    const { response } = await getPaginatedList("queue", "MatchIds", 500);
    return { MatchIds: response };
  },

  getSettings() {
    return apiGateway("settings");
  },

  processQueue() {
    return apiGateway("queue", { method: "del" });
  },

  putDataMapper(id, db, table, columns, roleArn, deleteOldVersions, format) {
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
        Format: format,
        RoleArn: roleArn,
        DeleteOldVersions: deleteOldVersions
      }
    });
  }
};

export default gateway;
