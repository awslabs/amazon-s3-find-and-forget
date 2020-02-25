# DefaultApi

All URIs are relative to *https://&lt;cloudfront-hostname&gt;/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addToDeletionQueue**](DefaultApi.md#addToDeletionQueue) | **PATCH** /queue | 
[**createDataMapper**](DefaultApi.md#createDataMapper) | **PUT** /data_mappers/{data_mapper_id} | 
[**deleteDataMapper**](DefaultApi.md#deleteDataMapper) | **DELETE** /data_mappers/{data_mapper_id} | 
[**deleteMatches**](DefaultApi.md#deleteMatches) | **DELETE** /queue/matches | 
[**getJob**](DefaultApi.md#getJob) | **GET** /jobs/{job_id} | 
[**getJobEvents**](DefaultApi.md#getJobEvents) | **GET** /jobs/{job_id}/events | 
[**getSettings**](DefaultApi.md#getSettings) | **GET** /settings | 
[**listDataMappers**](DefaultApi.md#listDataMappers) | **GET** /data_mappers | 
[**listDeletionQueueMatches**](DefaultApi.md#listDeletionQueueMatches) | **GET** /queue | 
[**listJobs**](DefaultApi.md#listJobs) | **GET** /jobs | 
[**startDeletionJob**](DefaultApi.md#startDeletionJob) | **DELETE** /queue | 


<a name="addToDeletionQueue"></a>
# **addToDeletionQueue**
> DeletionQueueItem addToDeletionQueue(createDeletionQueueItem)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **createDeletionQueueItem** | [**CreateDeletionQueueItem**](/Models/CreateDeletionQueueItem.md)|  |

### Return type

[**DeletionQueueItem**](/Models/DeletionQueueItem.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="createDataMapper"></a>
# **createDataMapper**
> createDataMapper(dataMapperId, createDataMapperHandler)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataMapperId** | **String**| Data Mapper ID | [default to null]
 **createDataMapperHandler** | [**CreateDataMapperHandler**](/Models/CreateDataMapperHandler.md)|  |

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

<a name="deleteDataMapper"></a>
# **deleteDataMapper**
> deleteDataMapper(dataMapperId)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataMapperId** | **String**| Data Mapper ID | [default to null]

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="deleteMatches"></a>
# **deleteMatches**
> deleteMatches(cancelItemsHandler)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **cancelItemsHandler** | [**CancelItemsHandler**](/Models/CancelItemsHandler.md)|  |

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

<a name="getJob"></a>
# **getJob**
> getJob(jobId)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **jobId** | **String**| Job ID | [default to null]

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="getJobEvents"></a>
# **getJobEvents**
> getJobEvents(jobId, startAt, pageSize)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **jobId** | **String**| Job ID | [default to null]
 **startAt** | **String**| Start at watermark query string parameter | [optional] [default to null]
 **pageSize** | **Integer**| Page size query string parameter | [optional] [default to null]

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="getSettings"></a>
# **getSettings**
> getSettings()



### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="listDataMappers"></a>
# **listDataMappers**
> listDataMappers()



### Parameters
This endpoint does not need any parameter.

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="listDeletionQueueMatches"></a>
# **listDeletionQueueMatches**
> inline_response_200 listDeletionQueueMatches()



### Parameters
This endpoint does not need any parameter.

### Return type

[**inline_response_200**](/Models/inline_response_200.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listJobs"></a>
# **listJobs**
> listJobs(startAt, pageSize)



### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **startAt** | **String**| Start at watermark query string parameter | [optional] [default to null]
 **pageSize** | **Integer**| Page size query string parameter | [optional] [default to null]

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="startDeletionJob"></a>
# **startDeletionJob**
> Job startDeletionJob()



### Parameters
This endpoint does not need any parameter.

### Return type

[**Job**](/Models/Job.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

