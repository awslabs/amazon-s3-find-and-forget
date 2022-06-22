# Documentation for Amazon S3 Find And Forget API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

API | Operation | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DataMapperApi* | [**DeleteDataMapper**](./Apis/DataMapperApi.md#deletedatamapper) | **DELETE** /v1/data_mappers/{data_mapper_id} | Removes a data mapper
*DataMapperApi* | [**GetDataMapper**](./Apis/DataMapperApi.md#getdatamapper) | **GET** /v1/data_mappers/{data_mapper_id} | Returns the details of a data mapper
*DataMapperApi* | [**ListDataMappers**](./Apis/DataMapperApi.md#listdatamappers) | **GET** /v1/data_mappers | Lists data mappers
*DataMapperApi* | [**PutDataMapper**](./Apis/DataMapperApi.md#putdatamapper) | **PUT** /v1/data_mappers/{data_mapper_id} | Creates or modifies a data mapper
*DeletionQueueApi* | [**AddItemToDeletionQueue**](./Apis/DeletionQueueApi.md#additemtodeletionqueue) | **PATCH** /v1/queue | Adds an item to the deletion queue (Deprecated: use PATCH /v1/queue/matches)
*DeletionQueueApi* | [**AddItemsToDeletionQueue**](./Apis/DeletionQueueApi.md#additemstodeletionqueue) | **PATCH** /v1/queue/matches | Adds one or more items to the deletion queue
*DeletionQueueApi* | [**DeleteMatches**](./Apis/DeletionQueueApi.md#deletematches) | **DELETE** /v1/queue/matches | Removes one or more items from the deletion queue
*DeletionQueueApi* | [**ListDeletionQueueMatches**](./Apis/DeletionQueueApi.md#listdeletionqueuematches) | **GET** /v1/queue | Lists deletion queue items
*DeletionQueueApi* | [**StartDeletionJob**](./Apis/DeletionQueueApi.md#startdeletionjob) | **DELETE** /v1/queue | Starts a job for the items in the deletion queue
*JobApi* | [**GetJob**](./Apis/JobApi.md#getjob) | **GET** /v1/jobs/{job_id} | Returns the details of a job
*JobApi* | [**GetJobEvents**](./Apis/JobApi.md#getjobevents) | **GET** /v1/jobs/{job_id}/events | Lists all events for a job
*JobApi* | [**ListJobs**](./Apis/JobApi.md#listjobs) | **GET** /v1/jobs | Lists all jobs
*SettingsApi* | [**GetSettings**](./Apis/SettingsApi.md#getsettings) | **GET** /v1/settings | Gets the solution settings


<a name="documentation-for-models"></a>
## Documentation for Models

 - [CreateDeletionQueueItem](./Models/CreateDeletionQueueItem.md)
 - [DataMapper](./Models/DataMapper.md)
 - [DataMapperQueryExecutorParameters](./Models/DataMapperQueryExecutorParameters.md)
 - [DeletionQueue](./Models/DeletionQueue.md)
 - [DeletionQueueItem](./Models/DeletionQueueItem.md)
 - [Error](./Models/Error.md)
 - [Job](./Models/Job.md)
 - [JobEvent](./Models/JobEvent.md)
 - [JobSummary](./Models/JobSummary.md)
 - [ListOfCreateDeletionQueueItems](./Models/ListOfCreateDeletionQueueItems.md)
 - [ListOfDataMappers](./Models/ListOfDataMappers.md)
 - [ListOfDeletionQueueItem](./Models/ListOfDeletionQueueItem.md)
 - [ListOfJobEvents](./Models/ListOfJobEvents.md)
 - [ListOfJobs](./Models/ListOfJobs.md)
 - [ListOfMatchDeletions](./Models/ListOfMatchDeletions.md)
 - [MatchDeletion](./Models/MatchDeletion.md)
 - [Settings](./Models/Settings.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

<a name="Authorizer"></a>
### Authorizer

- **Type**: API key
- **API key parameter name**: Authorization
- **Location**: HTTP header

Consult the [User Guide](../USER_GUIDE.md#making-authenticated-api-requests) to make authenticated requests.
