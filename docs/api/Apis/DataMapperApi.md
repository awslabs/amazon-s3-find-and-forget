# DataMapperApi

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**CreateDataMapper**](DataMapperApi.md#createdatamapper) | **PUT** /v1/data_mappers/{data_mapper_id} | Creates a data mapper
[**DeleteDataMapper**](DataMapperApi.md#deletedatamapper) | **DELETE** /v1/data_mappers/{data_mapper_id} | Removes a data mapper
[**ListDataMappers**](DataMapperApi.md#listdatamappers) | **GET** /v1/data_mappers | Lists data mappers


<a name="createdatamapper"></a>
## **CreateDataMapper**

Creates a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **DataMapperId** | **String**| Data Mapper ID path parameter | [default to null]
 **DataMapper** | [**DataMapper**](../Models/DataMapper.md)| Request body containing details of the Data Mapper to create |

### Return type

[**DataMapper**](../Models/DataMapper.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deletedatamapper"></a>
## **DeleteDataMapper**

Removes a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **DataMapperId** | **String**| Data Mapper ID path parameter | [default to null]

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

<a name="listdatamappers"></a>
## **ListDataMappers**

Lists data mappers

### Parameters
This endpoint does not need any parameters.

### Return type

[**ListOfDataMappers**](../Models/ListOfDataMappers.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

