# JobApi

All URIs are relative to *https://&lt;your-api-gw-url&gt;/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getJob**](JobApi.md#getJob) | **GET** /jobs/{job_id} | Returns the details of a job
[**getJobEvents**](JobApi.md#getJobEvents) | **GET** /jobs/{job_id}/events | Lists all events for a job
[**listJobs**](JobApi.md#listJobs) | **GET** /jobs | Lists all jobs




<a name="getJob"></a>
## **getJob**
> Job getJob(jobId)

Returns the details of a job

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **jobId** | **String**| Job ID path parameter | [default to null]


### Return type

[**Job**](../Models/Job.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: application/json, 


<a name="getJobEvents"></a>
## **getJobEvents**
> ListOfJobEvents getJobEvents(jobId, startAt, pageSize)

Lists all events for a job

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **jobId** | **String**| Job ID path parameter | [default to null]
 **startAt** | **String**| Start at watermark query string parameter | [optional] [default to 0]
 **pageSize** | **Integer**| Page size query string parameter | [optional] [default to null]


### Return type

[**ListOfJobEvents**](../Models/ListOfJobEvents.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: application/json, 


<a name="listJobs"></a>
## **listJobs**
> ListOfJobs listJobs(startAt, pageSize)

Lists all jobs

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **startAt** | **String**| Start at watermark query string parameter | [optional] [default to 0]
 **pageSize** | **Integer**| Page size query string parameter | [optional] [default to null]


### Return type

[**ListOfJobs**](../Models/ListOfJobs.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: application/json


