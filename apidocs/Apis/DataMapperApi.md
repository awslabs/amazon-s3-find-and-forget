# DataMapperApi

All URIs are relative to *https://&lt;your-api-gw-url&gt;/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createDataMapper**](DataMapperApi.md#createDataMapper) | **PUT** /data_mappers/{data_mapper_id} | 
[**deleteDataMapper**](DataMapperApi.md#deleteDataMapper) | **DELETE** /data_mappers/{data_mapper_id} | 
[**listDataMappers**](DataMapperApi.md#listDataMappers) | **GET** /data_mappers | 


<a name="createDataMapper"></a>
# **createDataMapper**
> DataMapper createDataMapper(dataMapperId, dataMapper)



    Creates a data mapper

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataMapperId** | **String**| Data Mapper ID | [default to null]
 **dataMapper** | [**DataMapper**](/Models/DataMapper.md)|  |

### Return type

[**DataMapper**](/Models/DataMapper.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteDataMapper"></a>
# **deleteDataMapper**
> deleteDataMapper(dataMapperId)



    Removes a data mapper

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

<a name="listDataMappers"></a>
# **listDataMappers**
> ListOfDataMappers listDataMappers()



    Lists data mappers

### Parameters
This endpoint does not need any parameter.

### Return type

[**ListOfDataMappers**](/Models/ListOfDataMappers.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

