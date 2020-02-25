# Documentation for S3F2 API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://<your-api-gw-url>/Prod*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DataMapperApi* | [**createDataMapper**](Apis/DataMapperApi.md#createdatamapper) | **PUT** /data_mappers/{data_mapper_id} | 
*DataMapperApi* | [**deleteDataMapper**](Apis/DataMapperApi.md#deletedatamapper) | **DELETE** /data_mappers/{data_mapper_id} | 
*DataMapperApi* | [**listDataMappers**](Apis/DataMapperApi.md#listdatamappers) | **GET** /data_mappers | 
*DeletionQueueApi* | [**addToDeletionQueue**](Apis/DeletionQueueApi.md#addtodeletionqueue) | **PATCH** /queue | 
*DeletionQueueApi* | [**deleteMatches**](Apis/DeletionQueueApi.md#deletematches) | **DELETE** /queue/matches | 
*DeletionQueueApi* | [**listDeletionQueueMatches**](Apis/DeletionQueueApi.md#listdeletionqueuematches) | **GET** /queue | 
*DeletionQueueApi* | [**startDeletionJob**](Apis/DeletionQueueApi.md#startdeletionjob) | **DELETE** /queue | 
*JobApi* | [**getJob**](Apis/JobApi.md#getjob) | **GET** /jobs/{job_id} | 
*JobApi* | [**getJobEvents**](Apis/JobApi.md#getjobevents) | **GET** /jobs/{job_id}/events | 
*JobApi* | [**listJobs**](Apis/JobApi.md#listjobs) | **GET** /jobs | 
*SettingsApi* | [**getSettings**](Apis/SettingsApi.md#getsettings) | **GET** /settings | 


<a name="documentation-for-models"></a>
## Documentation for Models

 - [/Models.CreateDeletionQueueItem](Models/CreateDeletionQueueItem.md)
 - [/Models.DataMapper](Models/DataMapper.md)
 - [/Models.DataMapperQueryExecutorParameters](Models/DataMapperQueryExecutorParameters.md)
 - [/Models.DeletionQueue](Models/DeletionQueue.md)
 - [/Models.DeletionQueueItem](Models/DeletionQueueItem.md)
 - [/Models.Error](Models/Error.md)
 - [/Models.Job](Models/Job.md)
 - [/Models.JobEvent](Models/JobEvent.md)
 - [/Models.ListOfDataMappers](Models/ListOfDataMappers.md)
 - [/Models.ListOfJobEvents](Models/ListOfJobEvents.md)
 - [/Models.ListOfJobs](Models/ListOfJobs.md)
 - [/Models.ListOfMatchDeletions](Models/ListOfMatchDeletions.md)
 - [/Models.MatchDeletion](Models/MatchDeletion.md)
 - [/Models.Settings](Models/Settings.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

<a name="CognitoAuthorizer"></a>
### CognitoAuthorizer

- **Type**: API key
- **API key parameter name**: Authorization
- **Location**: HTTP header

