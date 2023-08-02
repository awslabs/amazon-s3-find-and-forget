# Change Log

## v0.63 (unreleased)

- [#376](https://github.com/awslabs/amazon-s3-find-and-forget/issues/376):
  Upgrade backend dependencies
- [#377](https://github.com/awslabs/amazon-s3-find-and-forget/issues/377):
  Upgrade backend dependencies

## v0.62

- [#369](https://github.com/awslabs/amazon-s3-find-and-forget/issues/369):
  Upgrade frontend dependencies
- [#370](https://github.com/awslabs/amazon-s3-find-and-forget/issues/370):
  Upgrade frontend dependencies
- [#371](https://github.com/awslabs/amazon-s3-find-and-forget/issues/371):
  Upgrade frontend dependencies
- [#372](https://github.com/awslabs/amazon-s3-find-and-forget/issues/372):
  Upgrade backend dependencies

## v0.61

- [#368](https://github.com/awslabs/amazon-s3-find-and-forget/issues/368): Fix
  an issue that prevented deployments for users who have multiple S3F2 instances
  deployed within the same AWS account, using the same ResourcePrefix parameter,
  but in different regions.

## v0.60

- [#367](https://github.com/awslabs/amazon-s3-find-and-forget/pull/367): Fix
  deployment issue caused by deprecation of polling-based S3 sources in AWS
  CodePipeline
- [#366](https://github.com/awslabs/amazon-s3-find-and-forget/pull/366): Upgrade
  backend and frontend dependencies

## v0.59

- [#363](https://github.com/awslabs/amazon-s3-find-and-forget/pull/363): Bump
  cfn-lint version, update CloudFront to use OAC instead of OAI
- [#362](https://github.com/awslabs/amazon-s3-find-and-forget/pull/362):
  Improvements for container image build process
- [#360](https://github.com/awslabs/amazon-s3-find-and-forget/pull/360):
  Refactor of Web UI S3 bucket access control mechanisms

## v0.58

- [#359](https://github.com/awslabs/amazon-s3-find-and-forget/pull/359): Fix UI
  Links to documentation
- [#358](https://github.com/awslabs/amazon-s3-find-and-forget/pull/358): Fix for
  bug that caused failure when opening gzipped files due to pyarrow unzipping

## v0.57

- [#348](https://github.com/awslabs/amazon-s3-find-and-forget/pull/348): Cost
  and performance improvement for Queue API
- [#350](https://github.com/awslabs/amazon-s3-find-and-forget/pull/350): Upgrade
  build dependencies
- [#351](https://github.com/awslabs/amazon-s3-find-and-forget/pull/351): Upgrade
  frontend dependencies
- [#352](https://github.com/awslabs/amazon-s3-find-and-forget/pull/352): Upgrade
  frontend dependencies
- [#355](https://github.com/awslabs/amazon-s3-find-and-forget/pull/355): Upgrade
  frontend and backend dependencies

## v0.56

- [#343](https://github.com/awslabs/amazon-s3-find-and-forget/pull/343): Upgrade
  frontend dependencies
- [#345](https://github.com/awslabs/amazon-s3-find-and-forget/pull/345):
  Improved error handling when processing invalid match IDs during the Query
  Planning phase
- [#346](https://github.com/awslabs/amazon-s3-find-and-forget/pull/346): Upgrade
  build dependencies
- [#347](https://github.com/awslabs/amazon-s3-find-and-forget/pull/347): Add
  settings to allocate AWS Lambda memory size for API handlers and Deletion Job
  tasks

## v0.55

- [#342](https://github.com/awslabs/amazon-s3-find-and-forget/pull/342): Fix for
  an issue affecting jobs terminating with FORGET_PARTIALLY_FAILED despite
  correctly processing all the objects, caused by ECS running multi-processing
  in fork mode. It includes improvement on multi-processing handling, as well as
  enhanced logging to include PIDs when running on ECS during the Forget phase
- [#341](https://github.com/awslabs/amazon-s3-find-and-forget/pull/341): Upgrade
  build dependencies
- [#340](https://github.com/awslabs/amazon-s3-find-and-forget/pull/340): Upgrade
  frontend dependencies

## v0.54

- [#336](https://github.com/awslabs/amazon-s3-find-and-forget/pull/336): Add
  Mumbai (ap-south-1) deploy link

## v0.53

- [#332](https://github.com/awslabs/amazon-s3-find-and-forget/pull/332):
  - Switch to
    [Pyarrow's S3FileSystem](https://arrow.apache.org/docs/python/generated/pyarrow.fs.S3FileSystem.html)
    to read from S3 on the Forget Phase
  - Switch to
    [boto3's upload_fileobj](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_fileobj)
    to write to S3 on the Forget Phase
  - Upgrade backend dependencies
- [#327](https://github.com/awslabs/amazon-s3-find-and-forget/pull/327): Improve
  performance of Athena results handler
- [#329](https://github.com/awslabs/amazon-s3-find-and-forget/pull/329): Upgrade
  frontend dependencies

## v0.52

- [#318](https://github.com/awslabs/amazon-s3-find-and-forget/pull/318): Added
  support for AWS China
- [#324](https://github.com/awslabs/amazon-s3-find-and-forget/pull/324): Upgrade
  frontend dependencies

## v0.51

- [#321](https://github.com/awslabs/amazon-s3-find-and-forget/pull/321): Upgrade
  numpy dependency

## v0.50

- [#322](https://github.com/awslabs/amazon-s3-find-and-forget/pull/322):
  Upgraded to Python 3.9

## v0.49

- [#314](https://github.com/awslabs/amazon-s3-find-and-forget/pull/314): Fix
  query generation step for Composite matches consisting of a single column
- [#320](https://github.com/awslabs/amazon-s3-find-and-forget/pull/320): Fix
  deployment issue introduced in v0.48

## v0.48

- [#316](https://github.com/awslabs/amazon-s3-find-and-forget/pull/316): Upgrade
  dependencies
- [#313](https://github.com/awslabs/amazon-s3-find-and-forget/pull/313): Add
  option to choose IAM for authentication (in place of Cognito)
- [#313](https://github.com/awslabs/amazon-s3-find-and-forget/pull/313): Add
  option to not deploy WebUI component. Cognito auth is required for WebUI

## v0.47

- [#310](https://github.com/awslabs/amazon-s3-find-and-forget/pull/310): Improve
  performance of Athena query generation
- [#308](https://github.com/awslabs/amazon-s3-find-and-forget/pull/308): Upgrade
  frontend dependencies and use npm workspaces to link frontend sub-project

## v0.46

- [#306](https://github.com/awslabs/amazon-s3-find-and-forget/pull/306): Adds
  retry behaviour for old object deletion to improve reliability against
  transient errors from Amazon S3

## v0.45

- [#303](https://github.com/awslabs/amazon-s3-find-and-forget/pull/303): Improve
  performance of Athena query generation
- [#301](https://github.com/awslabs/amazon-s3-find-and-forget/pull/301): Include
  table name to error when query generation fails due to an invalid column type
- Dependency version updates for:
  - [#302](https://github.com/awslabs/amazon-s3-find-and-forget/pull/302)
    simple-plist
  - [#300](https://github.com/awslabs/amazon-s3-find-and-forget/pull/300) plist
  - [#299](https://github.com/awslabs/amazon-s3-find-and-forget/pull/299)
    ansi-regex
  - [#298](https://github.com/awslabs/amazon-s3-find-and-forget/pull/298)
    minimist
  - [#296](https://github.com/awslabs/amazon-s3-find-and-forget/pull/296)
    node-forge

## v0.44

- [#293](https://github.com/awslabs/amazon-s3-find-and-forget/pull/293): Upgrade
  dependencies

## v0.43

- [#289](https://github.com/awslabs/amazon-s3-find-and-forget/pull/289): Upgrade
  frontend dependencies

- [#287](https://github.com/awslabs/amazon-s3-find-and-forget/pull/287): Add
  data mapper parameter for ignoring Object Not Found exceptions encountered
  during deletion

## v0.42

- [#285](https://github.com/awslabs/amazon-s3-find-and-forget/pull/285): Fix for
  a bug that caused a job to fail with a false positive
  `The object s3://<REDACTED> was processed successfully but no rows required deletion`
  when processing a job with queries running for more than 30m

- [#286](https://github.com/awslabs/amazon-s3-find-and-forget/pull/286): Fix for
  a bug that causes `AthenaQueryMaxRetries` setting to be ignored

- [#286](https://github.com/awslabs/amazon-s3-find-and-forget/pull/286): Make
  state machine more resilient to transient failures by adding retry

- [#284](https://github.com/awslabs/amazon-s3-find-and-forget/pull/284): Improve
  performance of find query for data mappers with multiple column identifiers

## v0.41

- [#283](https://github.com/awslabs/amazon-s3-find-and-forget/pull/283): Fix for
  a bug that caused a job to fail with `Runtime.ExitError` when processing a
  large queue of objects to be modified

- [#281](https://github.com/awslabs/amazon-s3-find-and-forget/pull/281): Improve
  performance of query generation step for tables with many partitions

## v0.40

- [#280](https://github.com/awslabs/amazon-s3-find-and-forget/pull/280): Improve
  performance for large queues of composite matches

## v0.39

- [#279](https://github.com/awslabs/amazon-s3-find-and-forget/pull/279): Improve
  performance for large queues of simple matches and logging additions

## v0.38

- [#278](https://github.com/awslabs/amazon-s3-find-and-forget/pull/278): Fix for
  a bug that caused a job to fail if the processing of an object took longer
  than the lifetime of its IAM temporary access credentials

## v0.37

- [#276](https://github.com/awslabs/amazon-s3-find-and-forget/pull/276): First
  attempt for fixing a bug that causes the access token to expire and cause a
  Job to fail if processing of an object takes more than an hour

- [#275](https://github.com/awslabs/amazon-s3-find-and-forget/pull/275): Upgrade
  JavaScript dependencies

- [#274](https://github.com/awslabs/amazon-s3-find-and-forget/pull/274): Fix for
  a bug that causes deletion to fail in parquet files when a data mapper has
  multiple column identifiers

## v0.36

- [#272](https://github.com/awslabs/amazon-s3-find-and-forget/pull/272):
  Introduce a retry mechanism when running Athena queries

## v0.35

- [#271](https://github.com/awslabs/amazon-s3-find-and-forget/pull/271): Support
  for decimal type column identifiers in Parquet files

## v0.34

- [#270](https://github.com/awslabs/amazon-s3-find-and-forget/pull/270): Fix for
  a bug affecting the front-end causing a 403 error when making a request to STS
  in the Data Mappers Page

## v0.33

- [#266](https://github.com/awslabs/amazon-s3-find-and-forget/pull/266): Fix
  creating data mapper bug when glue table doesn't have partition keys

- [#264](https://github.com/awslabs/amazon-s3-find-and-forget/pull/264): Upgrade
  frontend dependencies

- [#263](https://github.com/awslabs/amazon-s3-find-and-forget/pull/263): Improve
  bucket policies

- [#261](https://github.com/awslabs/amazon-s3-find-and-forget/pull/261): Upgrade
  frontend dependencies

## v0.32

- [#260](https://github.com/awslabs/amazon-s3-find-and-forget/pull/260): Add
  Stockholm region

## v0.31

- [#245](https://github.com/awslabs/amazon-s3-find-and-forget/pull/245): CSE-KMS
  support

- [#259](https://github.com/awslabs/amazon-s3-find-and-forget/pull/259): Upgrade
  frontend dependencies

## v0.30

- [#257](https://github.com/awslabs/amazon-s3-find-and-forget/pull/257):
  Introduce data mapper setting to specify the partition keys to be used when
  querying the data during the Find Phase

## v0.29

- [#256](https://github.com/awslabs/amazon-s3-find-and-forget/pull/256): Upgrade
  backend dependencies

## v0.28

- [#252](https://github.com/awslabs/amazon-s3-find-and-forget/pull/252): Upgrade
  frontend and backend dependencies

## v0.27

- [#248](https://github.com/awslabs/amazon-s3-find-and-forget/pull/248): Fix for
  a bug affecting Deletion Jobs running for cross-account buckets
- [#246](https://github.com/awslabs/amazon-s3-find-and-forget/pull/246): Upgrade
  build dependencies

## v0.26

- [#244](https://github.com/awslabs/amazon-s3-find-and-forget/pull/244): Upgrade
  frontend dependencies
- [#243](https://github.com/awslabs/amazon-s3-find-and-forget/pull/243): Upgrade
  frontend and build dependencies

## v0.25

> This version introduces breaking changes to the API and Web UI. Please consult
> the
> [migrating from <=v0.24 to v0.25 guide](docs/UPGRADE_GUIDE.md#migrating-from-v024-to-v025)

- [#239](https://github.com/awslabs/amazon-s3-find-and-forget/pull/239): Remove
  limit on queue size for individual jobs.

## v0.24

- [#240](https://github.com/awslabs/amazon-s3-find-and-forget/pull/240): Add ECR
  API Endpoint and migrate to
  [Fargate Platform version 1.4.0](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/platform_versions.html#platform-version-migration)

## v0.23

- [#238](https://github.com/awslabs/amazon-s3-find-and-forget/pull/238): Upgrade
  frontend dependencies

## v0.22

- [#236](https://github.com/awslabs/amazon-s3-find-and-forget/pull/236): Export
  API Gateway URL + Deletion Queue Table Stream ARN from main CloudFormation
  Template

## v0.21

- [#232](https://github.com/awslabs/amazon-s3-find-and-forget/pull/232): Fix for
  a bug affecting the Frontend not rendering the Data Mappers list when a Glue
  Table associated to a Data Mapper gets deleted
- [#233](https://github.com/awslabs/amazon-s3-find-and-forget/pull/233): Add GET
  endpoint for specific data mapper
- [#234](https://github.com/awslabs/amazon-s3-find-and-forget/pull/234):
  Performance improvements for the query generation phase

## v0.20

- [#230](https://github.com/awslabs/amazon-s3-find-and-forget/pull/230): Upgrade
  frontend dependencies
- [#231](https://github.com/awslabs/amazon-s3-find-and-forget/pull/231): Upgrade
  aws-amplify dependency

## v0.19

- [#226](https://github.com/awslabs/amazon-s3-find-and-forget/pull/226): Support
  for Composite Match Ids
- [#227](https://github.com/awslabs/amazon-s3-find-and-forget/pull/227): Upgrade
  frontend dependencies

## v0.18

- [#223](https://github.com/awslabs/amazon-s3-find-and-forget/pull/223): This
  release fixes
  [an issue (#222)](https://github.com/awslabs/amazon-s3-find-and-forget/issues/222)
  where new deployments of the solution could fail due to unavailability of a
  third-party dependency. Container base images are now retrieved and bundled
  with each release.

## v0.17

- [#220](https://github.com/awslabs/amazon-s3-find-and-forget/pull/220): Fix for
  a bug affecting Parquet files with lower-cased column identifiers generating a
  `Apache Arrow processing error: 'Field "customerid" does not exist in table schema`
  exception during the Forget phase (for example `customerId` in parquet file
  being mapped to lower-case `customerid` in glue table)

## v0.16

- [#216](https://github.com/awslabs/amazon-s3-find-and-forget/pull/216): Fix for
  a bug affecting Parquet files with complex data types as column identifier
  generating a
  `Apache Arrow processing error: Mix of struct and list types not yet supported`
  exception during the Forget phase
- [#216](https://github.com/awslabs/amazon-s3-find-and-forget/pull/216): Fix for
  a bug affecting workgroups other than `primary` generating a permission error
  exception during the Find phase

## v0.15

- [#215](https://github.com/awslabs/amazon-s3-find-and-forget/pull/215): Support
  for data registered with AWS Lake Formation

## v0.14

- [#213](https://github.com/awslabs/amazon-s3-find-and-forget/pull/213): Fix for
  a bug causing a FIND_FAILED error related to a States.DataLimitExceed
  exception triggered by Step Function's Athena workflow when executing the
  SubmitQueryResults lambda
- [#208](https://github.com/awslabs/amazon-s3-find-and-forget/pull/208): Fix bug
  preventing PUT DataMapper to edit existing datamapper with same location, fix
  Front-end DataMapper creation to prevent editing an existing one.

## v0.13

- [#207](https://github.com/awslabs/amazon-s3-find-and-forget/pull/207): Upgrade
  frontend dependencies

## v0.12

- [#202](https://github.com/awslabs/amazon-s3-find-and-forget/pull/202): Fix a
  bug that was affecting Partitions with non-string types generating a
  `SYNTAX_ERROR: line x:y: '=' cannot be applied to integer, varchar(z)`
  exception during the Find Phase
- [#203](https://github.com/awslabs/amazon-s3-find-and-forget/pull/203): Upgrade
  frontend dependencies
- [#204](https://github.com/awslabs/amazon-s3-find-and-forget/pull/204): Improve
  performance during Cleanup Phase
- [#205](https://github.com/awslabs/amazon-s3-find-and-forget/pull/205): Fix a
  UI issue affecting FireFox preventing to show the correct queue size due to a
  missing CORS header

## v0.11

- [#200](https://github.com/awslabs/amazon-s3-find-and-forget/pull/200): Add API
  Endpoint for adding deletion queue items in batch - deprecates PATCH /v1/queue
- [#170](https://github.com/awslabs/amazon-s3-find-and-forget/pull/170): JSON
  support

## v0.10

- [#193](https://github.com/awslabs/amazon-s3-find-and-forget/pull/193): Add
  support for datasets with Pandas indexes. Pandas indexes will be preserved if
  present.
- [#194](https://github.com/awslabs/amazon-s3-find-and-forget/pull/194): Remove
  debugging code from Fargate task
- [#195](https://github.com/awslabs/amazon-s3-find-and-forget/pull/195): Fix
  support for requester pays buckets
- [#196](https://github.com/awslabs/amazon-s3-find-and-forget/pull/196): Upgrade
  backend dependencies
- [#197](https://github.com/awslabs/amazon-s3-find-and-forget/pull/197): Fix
  duplicated query executions during Find Phase

## v0.9

> This version introduces breaking changes to the CloudFormation templates.
> Please consult the
> [migrating from <=v0.8 to v0.9 guide](docs/UPGRADE_GUIDE.md#migrating-from-v08-to-v09)

- [#189](https://github.com/awslabs/amazon-s3-find-and-forget/pull/189): UI
  Updates
- [#191](https://github.com/awslabs/amazon-s3-find-and-forget/pull/191): Deploy
  VPC template by default

## v0.8

- [#185](https://github.com/awslabs/amazon-s3-find-and-forget/pull/185): Fix
  dead links to VPC info in docs
- [#186](https://github.com/awslabs/amazon-s3-find-and-forget/pull/186): Fix:
  Solves an issue where the forget phase container could crash when redacting
  numeric Match IDs from its logs
- [#187](https://github.com/awslabs/amazon-s3-find-and-forget/pull/187):
  Dependency version updates for react-scripts

## v0.7

- [#183](https://github.com/awslabs/amazon-s3-find-and-forget/pull/183):
  Dependency version updates for elliptic

## v0.6

- [#173](https://github.com/awslabs/amazon-s3-find-and-forget/pull/173): Show
  column types and hierarchy in the front-end during Data Mapper creation
- [#173](https://github.com/awslabs/amazon-s3-find-and-forget/pull/173): Add
  support for char, smallint, tinyint, double, float
- [#174](https://github.com/awslabs/amazon-s3-find-and-forget/pull/174): Add
  support for types nested in struct
- [#177](https://github.com/awslabs/amazon-s3-find-and-forget/pull/177):
  Reformat of Python source code (non-functional change)
- Dependency version updates for:
  - [#178](https://github.com/awslabs/amazon-s3-find-and-forget/pull/178),
    [#180](https://github.com/awslabs/amazon-s3-find-and-forget/pull/180) lodash
  - [#179](https://github.com/awslabs/amazon-s3-find-and-forget/pull/179)
    websocket-extensions

## v0.5

- [#172](https://github.com/awslabs/amazon-s3-find-and-forget/pull/172): Fix for
  an issue where Make may not install the required Lambda layer dependencies,
  resulting in unusable builds.

## v0.4

- [#171](https://github.com/awslabs/amazon-s3-find-and-forget/pull/171): Fix for
  a bug affecting the API for 5xx responses not returning the appropriate CORS
  headers

## v0.3

- [#164](https://github.com/awslabs/amazon-s3-find-and-forget/pull/164): Fix for
  a bug affecting v0.2 deployment via CloudFormation

## v0.2

- [#161](https://github.com/awslabs/amazon-s3-find-and-forget/pull/161): Fix for
  a bug affecting Parquet files with nullable values generating a
  `Table schema does not match schema used to create file` exception during the
  Forget phase

## v0.1

Initial Release
