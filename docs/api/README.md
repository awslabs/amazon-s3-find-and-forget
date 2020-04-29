# Documentation for Amazon S3 Find And Forget API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

API | Operation | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DataMapperApi* | [**CreateDataMapper**](./Apis/DataMapperApi.md#createdatamapper) | **PUT** /v1/data_mappers/{data_mapper_id} | Creates a data mapper
*DataMapperApi* | [**DeleteDataMapper**](./Apis/DataMapperApi.md#deletedatamapper) | **DELETE** /v1/data_mappers/{data_mapper_id} | Removes a data mapper
*DataMapperApi* | [**ListDataMappers**](./Apis/DataMapperApi.md#listdatamappers) | **GET** /v1/data_mappers | Lists data mappers
*DeletionQueueApi* | [**AddToDeletionQueue**](./Apis/DeletionQueueApi.md#addtodeletionqueue) | **PATCH** /v1/queue | Adds an item to the deletion queue
*DeletionQueueApi* | [**DeleteMatches**](./Apis/DeletionQueueApi.md#deletematches) | **DELETE** /v1/queue/matches | Removes an item from the deletion queue
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
 - [ListOfDataMappers](./Models/ListOfDataMappers.md)
 - [ListOfJobEvents](./Models/ListOfJobEvents.md)
 - [ListOfJobs](./Models/ListOfJobs.md)
 - [ListOfMatchDeletions](./Models/ListOfMatchDeletions.md)
 - [MatchDeletion](./Models/MatchDeletion.md)
 - [Settings](./Models/Settings.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

<a name="CognitoAuthorizer"></a>
### CognitoAuthorizer

- **Type**: API key
- **API key parameter name**: Authorization
- **Location**: HTTP header

