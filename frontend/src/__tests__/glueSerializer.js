import { bucketMapper, glueSerializer } from "../utils/glueSerializer";

test("it should serialize dbs and tables", () => {
  const table1 = {
    CreateTime: 1.571744695e9,
    DatabaseName: "db2",
    IsRegisteredWithLakeFormation: false,
    LastAccessTime: 0.0,
    Name: "table1",
    Owner: "hadoop",
    Parameters: {
      EXTERNAL: "TRUE",
      has_encrypted_data: "false",
      transient_lastDdlTime: "1571744695"
    },
    PartitionKeys: [{ Name: "product_category", Type: "string" }],
    Retention: 0,
    StorageDescriptor: {
      BucketColumns: [],
      Columns: [{ Name: "customer_id", Type: "string" }],
      Compressed: false,
      InputFormat: "org.apache.hadoop.mapred.TextInputFormat",
      Location: "s3://my-s3-bucket/parquet/",
      NumberOfBuckets: -1,
      OutputFormat:
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      Parameters: {},
      SerdeInfo: {
        Parameters: { "serialization.format": "1" },
        SerializationLibrary:
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      },
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
  };

  const table2 = {
    CreateTime: 1.571670199e9,
    CreatedBy: "arn:aws:sts::123456123456:assumed-role/Admin/foo",
    DatabaseName: "db2",
    IsRegisteredWithLakeFormation: false,
    LastAccessTime: 0.0,
    Name: "table2",
    Owner: "hadoop",
    Parameters: {
      EXTERNAL: "TRUE",
      has_encrypted_data: "false",
      transient_lastDdlTime: "1571670199"
    },
    PartitionKeys: [],
    Retention: 0,
    StorageDescriptor: {
      BucketColumns: [],
      Columns: [{ Name: "author", Type: "string" }],
      Compressed: false,
      InputFormat: "org.apache.hadoop.mapred.TextInputFormat",
      Location: "s3://my-s3-bucket/parquet2/",
      NumberOfBuckets: -1,
      OutputFormat:
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      Parameters: {},
      SerdeInfo: {
        Parameters: { "serialization.format": "1" },
        SerializationLibrary:
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      },
      SkewedInfo: {
        SkewedColumnNames: [],
        SkewedColumnValueLocationMaps: {},
        SkewedColumnValues: []
      },
      SortColumns: [],
      StoredAsSubDirectories: false
    },
    TableType: "EXTERNAL_TABLE",
    UpdateTime: 1.571670199e9
  };

  const table3 = {
    CreateTime: 1.571744695e9,
    DatabaseName: "db3",
    IsRegisteredWithLakeFormation: false,
    LastAccessTime: 0.0,
    Name: "table5",
    Owner: "hadoop",
    Parameters: {
      EXTERNAL: "TRUE",
      has_encrypted_data: "false",
      transient_lastDdlTime: "1571744695"
    },
    PartitionKeys: [{ Name: "product_category", Type: "string" }],
    Retention: 0,
    StorageDescriptor: {
      BucketColumns: [],
      Columns: [{ Name: "customer_id", Type: "string" }],
      Compressed: false,
      InputFormat: "org.apache.hadoop.mapred.TextInputFormat",
      Location: "s3://my-s3-bucket/parquet56/",
      NumberOfBuckets: -1,
      OutputFormat:
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      Parameters: {},
      SerdeInfo: {
        Parameters: { "serialization.format": "1" },
        SerializationLibrary:
          "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      },
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
  };

  const jsonTable = {
    CreateTime: 1.572961397e9,
    CreatedBy:
      "arn:aws:sts::123456123456:assumed-role/AWSGlueServiceRole-test/AWS-Crawler",
    DatabaseName: "db3",
    IsRegisteredWithLakeFormation: false,
    LastAccessTime: 1.572961397e9,
    Name: "table3",
    Owner: "owner",
    Parameters: {
      CrawlerSchemaDeserializerVersion: "1.0",
      CrawlerSchemaSerializerVersion: "1.0",
      UPDATED_BY_CRAWLER: "table3",
      averageRecordSize: "2471",
      classification: "json",
      compressionType: "gzip",
      objectCount: "744",
      recordCount: "4795154",
      sizeKey: "11026096613",
      typeOfData: "file"
    },
    PartitionKeys: [
      { Name: "partition_0", Type: "string" },
      { Name: "partition_1", Type: "string" },
      { Name: "partition_2", Type: "string" }
    ],
    Retention: 0,
    StorageDescriptor: {
      BucketColumns: [],
      Columns: [
        { Name: "id", Type: "string" },
        { Name: "type", Type: "string" },
        { Name: "repo", Type: "struct<id:int,name:string,url:string>" },
        { Name: "public", Type: "boolean" },
        { Name: "created_at", Type: "string" }
      ],
      Compressed: true,
      InputFormat: "org.apache.hadoop.mapred.TextInputFormat",
      Location:
        "s3://aws-glue-datasets-eu-west-1/examples/githubarchive/month/data/",
      NumberOfBuckets: -1,
      OutputFormat:
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      Parameters: {
        CrawlerSchemaDeserializerVersion: "1.0",
        CrawlerSchemaSerializerVersion: "1.0",
        UPDATED_BY_CRAWLER: "table3",
        averageRecordSize: "2471",
        classification: "json",
        compressionType: "gzip",
        objectCount: "744",
        recordCount: "4795154",
        sizeKey: "11026096613",
        typeOfData: "file"
      },
      SerdeInfo: {
        Parameters: {
          paths: "created_at,id,public,repo,type"
        },
        SerializationLibrary: "org.openx.data.jsonserde.JsonSerDe"
      },
      SortColumns: [],
      StoredAsSubDirectories: false
    },
    TableType: "EXTERNAL_TABLE",
    UpdateTime: 1.572961397e9
  };

  const tables = [
    { TableList: [] },
    { TableList: [table1, table2] },
    { TableList: [table3, jsonTable] }
  ];

  const expected = {
    databases: [
      {
        name: "db2",
        tables: [
          { name: "table1", columns: ["customer_id"] },
          { name: "table2", columns: ["author"] }
        ]
      },
      { name: "db3", tables: [{ name: "table5", columns: ["customer_id"] }] }
    ]
  };

  expect(glueSerializer(tables)).toEqual(expected);

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
