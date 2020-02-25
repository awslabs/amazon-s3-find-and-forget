# Documentation for S3F2 API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *https://<cloudfront-hostname>/Prod*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DefaultApi* | [**addToDeletionQueue**](Apis/DefaultApi.md#addtodeletionqueue) | **PATCH** /queue | 
*DefaultApi* | [**createDataMapper**](Apis/DefaultApi.md#createdatamapper) | **PUT** /data_mappers/{data_mapper_id} | 
*DefaultApi* | [**deleteDataMapper**](Apis/DefaultApi.md#deletedatamapper) | **DELETE** /data_mappers/{data_mapper_id} | 
*DefaultApi* | [**deleteMatches**](Apis/DefaultApi.md#deletematches) | **DELETE** /queue/matches | 
*DefaultApi* | [**getJob**](Apis/DefaultApi.md#getjob) | **GET** /jobs/{job_id} | 
*DefaultApi* | [**getJobEvents**](Apis/DefaultApi.md#getjobevents) | **GET** /jobs/{job_id}/events | 
*DefaultApi* | [**getSettings**](Apis/DefaultApi.md#getsettings) | **GET** /settings | 
*DefaultApi* | [**listDataMappers**](Apis/DefaultApi.md#listdatamappers) | **GET** /data_mappers | 
*DefaultApi* | [**listDeletionQueueMatches**](Apis/DefaultApi.md#listdeletionqueuematches) | **GET** /queue | 
*DefaultApi* | [**listJobs**](Apis/DefaultApi.md#listjobs) | **GET** /jobs | 
*DefaultApi* | [**startDeletionJob**](Apis/DefaultApi.md#startdeletionjob) | **DELETE** /queue | 


<a name="documentation-for-models"></a>
## Documentation for Models

 - [/Models.CancelItemsHandler](Models/CancelItemsHandler.md)
 - [/Models.CreateDataMapperHandler](Models/CreateDataMapperHandler.md)
 - [/Models.CreateDeletionQueueItem](Models/CreateDeletionQueueItem.md)
 - [/Models.DataMappersDataMapperIdQueryExecutorParameters](Models/DataMappersDataMapperIdQueryExecutorParameters.md)
 - [/Models.DeletionQueueItem](Models/DeletionQueueItem.md)
 - [/Models.Error](Models/Error.md)
 - [/Models.InlineResponse200](Models/InlineResponse200.md)
 - [/Models.Job](Models/Job.md)
 - [/Models.JobEvent](Models/JobEvent.md)
 - [/Models.QueueMatchesMatches](Models/QueueMatchesMatches.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

<a name="CognitoAuthorizer"></a>
### CognitoAuthorizer

- **Type**: API key
- **API key parameter name**: Authorization
- **Location**: HTTP header

