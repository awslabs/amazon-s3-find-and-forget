# Documentation for S3F2 API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

API | Operation | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DataMapperApi* | [**createDataMapper**](./Apis/DataMapperApi.md#createDataMapper) | **PUT** /data_mappers/{data_mapper_id} | Creates a data mapper
*DataMapperApi* | [**deleteDataMapper**](./Apis/DataMapperApi.md#deleteDataMapper) | **DELETE** /data_mappers/{data_mapper_id} | Removes a data mapper
*DataMapperApi* | [**listDataMappers**](./Apis/DataMapperApi.md#listDataMappers) | **GET** /data_mappers | Lists data mappers
*DeletionQueueApi* | [**addToDeletionQueue**](./Apis/DeletionQueueApi.md#addToDeletionQueue) | **PATCH** /queue | Adds an item to the deletion queue
*DeletionQueueApi* | [**deleteMatches**](./Apis/DeletionQueueApi.md#deleteMatches) | **DELETE** /queue/matches | Removes an item from the deletion queue
*DeletionQueueApi* | [**listDeletionQueueMatches**](./Apis/DeletionQueueApi.md#listDeletionQueueMatches) | **GET** /queue | Lists deletion queue items
*DeletionQueueApi* | [**startDeletionJob**](./Apis/DeletionQueueApi.md#startDeletionJob) | **DELETE** /queue | Starts a job for the items in the deletion queue
*JobApi* | [**getJob**](./Apis/JobApi.md#getJob) | **GET** /jobs/{job_id} | Returns the details of a job
*JobApi* | [**getJobEvents**](./Apis/JobApi.md#getJobEvents) | **GET** /jobs/{job_id}/events | Lists all events for a job
*JobApi* | [**listJobs**](./Apis/JobApi.md#listJobs) | **GET** /jobs | Lists all jobs
*SettingsApi* | [**getSettings**](./Apis/SettingsApi.md#getSettings) | **GET** /settings | Gets the solution settings


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

