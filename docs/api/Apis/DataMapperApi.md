# DataMapperApi

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**DeleteDataMapper**](DataMapperApi.md#deletedatamapper) | **DELETE** /v1/data_mappers/{data_mapper_id} | Removes a data mapper
[**GetDataMapper**](DataMapperApi.md#getdatamapper) | **GET** /v1/data_mappers/{data_mapper_id} | Returns the details of a data mapper
[**ListDataMappers**](DataMapperApi.md#listdatamappers) | **GET** /v1/data_mappers | Lists data mappers
[**PutDataMapper**](DataMapperApi.md#putdatamapper) | **PUT** /v1/data_mappers/{data_mapper_id} | Creates or modifies a data mapper


<a name="deletedatamapper"></a>
## **DeleteDataMapper**

Removes a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **Data\_mapper\_id** | **String**| Data Mapper ID path parameter | [default to null]

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="getdatamapper"></a>
## **GetDataMapper**

Returns the details of a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **Data\_mapper\_id** | **String**| Data Mapper ID path parameter | [default to null]

### Return type

[**DataMapper**](..Models/DataMapper.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listdatamappers"></a>
## **ListDataMappers**

Lists data mappers

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **Start\_at** | **String**| Start at watermark query string parameter | [optional] [default to 0]
 **Page\_size** | **Integer**| Page size query string parameter. Min: 1. Max: 1000 | [optional] [default to null]

### Return type

[**ListOfDataMappers**](..Models/ListOfDataMappers.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="putdatamapper"></a>
## **PutDataMapper**

Creates or modifies a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **Data\_mapper\_id** | **String**| Data Mapper ID path parameter | [default to null]
 **DataMapper** | [**DataMapper**](..Models/DataMapper.md)| Request body containing details of the Data Mapper to create or modify |

### Return type

[**DataMapper**](..Models/DataMapper.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

