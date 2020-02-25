# DataMapperApi

All URIs are relative to *https://&lt;your-api-gw-url&gt;/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createDataMapper**](DataMapperApi.md#createDataMapper) | **PUT** /data_mappers/{data_mapper_id} | Creates a data mapper
[**deleteDataMapper**](DataMapperApi.md#deleteDataMapper) | **DELETE** /data_mappers/{data_mapper_id} | Removes a data mapper
[**listDataMappers**](DataMapperApi.md#listDataMappers) | **GET** /data_mappers | Lists data mappers




<a name="createDataMapper"></a>
## **createDataMapper**
> DataMapper createDataMapper(dataMapperId, dataMapper)

Creates a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataMapperId** | **String**| Data Mapper ID path parameter | [default to null]
 **dataMapper** | [**DataMapper**](../Models/DataMapper.md)| Request body containing details of the Data Mapper to create |


### Return type

[**DataMapper**](../Models/DataMapper.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json, 
- **Accept**: application/json, 


<a name="deleteDataMapper"></a>
## **deleteDataMapper**
> deleteDataMapper(dataMapperId)

Removes a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataMapperId** | **String**| Data Mapper ID path parameter | [default to null]


### Return type



### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: 


<a name="listDataMappers"></a>
## **listDataMappers**
> ListOfDataMappers listDataMappers()

Lists data mappers

### Parameters
This endpoint does not need any parameters.


### Return type

[**ListOfDataMappers**](../Models/ListOfDataMappers.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: application/json


