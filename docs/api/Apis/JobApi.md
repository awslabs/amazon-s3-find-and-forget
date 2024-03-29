# JobApi

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetJob**](JobApi.md#getjob) | **GET** /v1/jobs/{job_id} | Returns the details of a job
[**GetJobEvents**](JobApi.md#getjobevents) | **GET** /v1/jobs/{job_id}/events | Lists all events for a job
[**ListJobs**](JobApi.md#listjobs) | **GET** /v1/jobs | Lists all jobs


<a name="getjob"></a>
## **GetJob**

Returns the details of a job

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **JobId** | **String**| Job ID path parameter | [default to null]

### Return type

[**Job**](../Models/Job.md)

### Authorization

[Authorizer](../README.md#Authorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getjobevents"></a>
## **GetJobEvents**

Lists all events for a job

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **JobId** | **String**| Job ID path parameter | [default to null]
 **StartAt** | **String**| Start at watermark query string parameter | [optional] [default to 0]
 **PageSize** | **Integer**| Page size query string parameter. Min: 1. Max: 1000 | [optional] [default to null]
 **Filter** | [**oneOf&lt;string,array&gt;**](../Models/.md)| Filters to apply in the format [key][operator][value]. If multiple filters are supplied, they will applied on an **AND** basis. Supported keys: EventName. Supported Operators: &#x3D;  | [optional] [default to null]

### Return type

[**ListOfJobEvents**](../Models/ListOfJobEvents.md)

### Authorization

[Authorizer](../README.md#Authorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listjobs"></a>
## **ListJobs**

Lists all jobs

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **StartAt** | **String**| Start at watermark query string parameter | [optional] [default to 0]
 **PageSize** | **Integer**| Page size query string parameter. Min: 1. Max: 1000 | [optional] [default to null]

### Return type

[**ListOfJobs**](../Models/ListOfJobs.md)

### Authorization

[Authorizer](../README.md#Authorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

