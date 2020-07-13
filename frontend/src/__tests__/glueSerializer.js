import { bucketMapper, glueSerializer } from "../utils/glueSerializer";

const tableMaker = ({
  dbname,
  tablename,
  partitions = [],
  columns = [],
  location,
  serde = {
    Parameters: { "serialization.format": "1" },
    SerializationLibrary:
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  }
}) => ({
  CreateTime: 1.571744695e9,
  DatabaseName: dbname,
  IsRegisteredWithLakeFormation: false,
  LastAccessTime: 0.0,
  Name: tablename,
  Owner: "hadoop",
  Parameters: {
    EXTERNAL: "TRUE",
    has_encrypted_data: "false",
    transient_lastDdlTime: "1571744695"
  },
  PartitionKeys: partitions,
  Retention: 0,
  StorageDescriptor: {
    BucketColumns: [],
    Columns: columns,
    Compressed: false,
    InputFormat: "org.apache.hadoop.mapred.TextInputFormat",
    Location: location,
    NumberOfBuckets: -1,
    OutputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    Parameters: {},
    SerdeInfo: serde,
    SkewedInfo: {
      SkewedColumnNames: [],
      SkewedColumnValueLocationMaps: {},
      SkewedColumnValues: []
    },
    SortColumns: [],
    StoredAsSubDirectories: false
  },
  TableType: "EXTERNAL_TABLE",
  UpdateTime: 1.572537815e9
});

const table1 = tableMaker({
  dbname: "db2",
  tablename: "table1",
  partitions: [{ Name: "product_category", Type: "string" }],
  columns: [{ Name: "customer_id", Type: "string" }],
  location: "s3://my-s3-bucket/parquet/"
});

const table2 = tableMaker({
  dbname: "db2",
  tablename: "table2",
  columns: [{ Name: "author", Type: "string" }],
  location: "s3://my-s3-bucket/parquet2/"
});

const table3 = tableMaker({
  dbname: "db3",
  tablename: "table5",
  columns: [{ Name: "customer_id", Type: "string" }],
  partitions: [{ Name: "product_category", Type: "string" }],
  location: "s3://my-s3-bucket/parquet56/"
});

const jsonTable = tableMaker({
  dbname: "db3",
  tablename: "table3",
  columns: [
    { Name: "id", Type: "string" },
    { Name: "type", Type: "string" },
    { Name: "repo", Type: "struct<id:int,name:string,url:string>" },
    { Name: "public", Type: "boolean" },
    { Name: "created_at", Type: "string" }
  ],
  partitions: [
    { Name: "partition_0", Type: "string" },
    { Name: "partition_1", Type: "string" },
    { Name: "partition_2", Type: "string" }
  ],
  location:
    "s3://aws-glue-datasets-eu-west-1/examples/githubarchive/month/data/",
  serde: {
    Parameters: {
      paths: "created_at,id,public,repo,type"
    },
    SerializationLibrary: "org.openx.data.jsonserde.JsonSerDe"
  }
});

const complexColumnsTable = tableMaker({
  dbname: "db4",
  tablename: "complex",
  columns: [
    { Name: "id", Type: "string" },
    { Name: "type", Type: "string" },
    { Name: "repo", Type: "struct<id:int,name:string,url:string>" },
    { Name: "simplearr", Type: "array<int>" },
    { Name: "arr", Type: "array<struct<field:int,n:string>>" },
    {
      Name: "nestedstruct",
      Type:
        "struct<a:int,b:string,c:struct<d:int,e:struct<f:int>>,g:struct<h:string>>"
    },
    {
      Name: "structandarr",
      Type:
        "struct<a:int,b:string,c:struct<d:int,e:struct<f:int>>,g:struct<h:string>,i:array<struct<l:int,m:struct<n:string>>>>"
    }
  ],
  partitions: [],
  location: "s3://my-s3-bucket/parquet/"
});

test("it should serialize dbs and tables", () => {
  const tables = [
    { TableList: [] },
    { TableList: [table1, table2] },
    { TableList: [table3, jsonTable] },
    { TableList: [complexColumnsTable] }
  ];

  const expected = {
    databases: [
      {
        name: "db2",
        tables: [
          {
            name: "table1",
            columns: [
              { name: "customer_id", type: "string", canBeIdentifier: true }
            ]
          },
          {
            name: "table2",
            columns: [{ name: "author", type: "string", canBeIdentifier: true }]
          }
        ]
      },
      {
        name: "db3",
        tables: [
          {
            name: "table5",
            columns: [
              { name: "customer_id", type: "string", canBeIdentifier: true }
            ]
          }
        ]
      },
      {
        name: "db4",
        tables: [
          {
            name: "complex",
            columns: [
              { name: "id", type: "string", canBeIdentifier: true },
              { name: "type", type: "string", canBeIdentifier: true },
              {
                name: "repo",
                type: "struct",
                canBeIdentifier: false,
                children: [
                  { name: "id", type: "int", canBeIdentifier: true },
                  { name: "name", type: "string", canBeIdentifier: true },
                  { name: "url", type: "string", canBeIdentifier: true }
                ]
              },
              { name: "simplearr", type: "array<int>", canBeIdentifier: false },
              {
                name: "arr",
                type: "array<struct>",
                canBeIdentifier: false,
                children: [
                  { name: "field", type: "int", canBeIdentifier: false },
                  { name: "n", type: "string", canBeIdentifier: false }
                ]
              },
              {
                name: "nestedstruct",
                type: "struct",
                canBeIdentifier: false,
                children: [
                  { name: "a", type: "int", canBeIdentifier: true },
                  { name: "b", type: "string", canBeIdentifier: true },
                  {
                    name: "c",
                    type: "struct",
                    canBeIdentifier: false,
                    children: [
                      { name: "d", type: "int", canBeIdentifier: true },
                      {
                        name: "e",
                        type: "struct",
                        canBeIdentifier: false,
                        children: [
                          { name: "f", type: "int", canBeIdentifier: true }
                        ]
                      }
                    ]
                  },
                  {
                    name: "g",
                    type: "struct",
                    canBeIdentifier: false,
                    children: [
                      {
                        name: "h",
                        type: "string",
                        canBeIdentifier: true
                      }
                    ]
                  }
                ]
              },
              {
                name: "structandarr",
                type: "struct",
                canBeIdentifier: false,
                children: [
                  { name: "a", type: "int", canBeIdentifier: true },
                  { name: "b", type: "string", canBeIdentifier: true },
                  {
                    name: "c",
                    type: "struct",
                    canBeIdentifier: false,
                    children: [
                      { name: "d", type: "int", canBeIdentifier: true },
                      {
                        name: "e",
                        type: "struct",
                        canBeIdentifier: false,
                        children: [
                          { name: "f", type: "int", canBeIdentifier: true }
                        ]
                      }
                    ]
                  },
                  {
                    name: "g",
                    type: "struct",
                    canBeIdentifier: false,
                    children: [
                      {
                        name: "h",
                        type: "string",
                        canBeIdentifier: true
                      }
                    ]
                  },
                  {
                    name: "i",
                    type: "array<struct>",
                    canBeIdentifier: false,
                    children: [
                      {
                        name: "l",
                        type: "int",
                        canBeIdentifier: false
                      },
                      {
                        name: "m",
                        type: "struct",
                        canBeIdentifier: false,
                        children: [
                          {
                            name: "n",
                            type: "string",
                            canBeIdentifier: false
                          }
                        ]
                      }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  };

  expect(glueSerializer(tables)).toEqual(expected);
});

test("it should throw error when deserializing a invalid table", () => {
  const brokenTable = tableMaker({
    dbname: "db5",
    tablename: "broken",
    columns: [
      {
        Name: "brokenstruct",
        Type: "struct<a:int"
      }
    ],
    partitions: [],
    location: "s3://my-s3-bucket/parquet/"
  });

  const failureScenario = [{ TableList: [brokenTable] }];
  try {
    glueSerializer(failureScenario);
  } catch (e) {
    expect(e).toBeInstanceOf(Error);
    expect(e).toHaveProperty("message", "Column schema is not valid");
  }
});

test("it should group buckets by table", () => {
  const getTableResponseArray = [table1, table2, table3, jsonTable].map(t => ({
    Table: t
  }));

  expect(bucketMapper(getTableResponseArray)).toEqual({
    "db2/table1": {
      bucket: "my-s3-bucket",
      location: "s3://my-s3-bucket/parquet/"
    },
    "db2/table2": {
      bucket: "my-s3-bucket",
      location: "s3://my-s3-bucket/parquet2/"
    },
    "db3/table5": {
      bucket: "my-s3-bucket",
      location: "s3://my-s3-bucket/parquet56/"
    },
    "db3/table3": {
      bucket: "aws-glue-datasets-eu-west-1",
      location:
        "s3://aws-glue-datasets-eu-west-1/examples/githubarchive/month/data/"
    }
  });
});
