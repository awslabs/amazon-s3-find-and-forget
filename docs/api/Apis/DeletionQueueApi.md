# DeletionQueueApi

All URIs are relative to *https://your-apigw-id.execute-api.region.amazonaws.com/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**AddToDeletionQueue**](DeletionQueueApi.md#addtodeletionqueue) | **PATCH** /queue | Adds an item to the deletion queue
[**DeleteMatches**](DeletionQueueApi.md#deletematches) | **DELETE** /queue/matches | Removes an item from the deletion queue
[**ListDeletionQueueMatches**](DeletionQueueApi.md#listdeletionqueuematches) | **GET** /queue | Lists deletion queue items
[**StartDeletionJob**](DeletionQueueApi.md#startdeletionjob) | **DELETE** /queue | Starts a job for the items in the deletion queue


<a name="addtodeletionqueue"></a>
## **AddToDeletionQueue**

Adds an item to the deletion queue

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **CreateDeletionQueueItem** | [**CreateDeletionQueueItem**](../Models/CreateDeletionQueueItem.md)| Request body containing details of the Match to add to the Deletion Queue |

### Return type

[**DeletionQueueItem**](../Models/DeletionQueueItem.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deletematches"></a>
## **DeleteMatches**

Removes an item from the deletion queue

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ListOfMatchDeletions** | [**ListOfMatchDeletions**](../Models/ListOfMatchDeletions.md)|  |

### Return type

null (empty response body)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

<a name="listdeletionqueuematches"></a>
## **ListDeletionQueueMatches**

Lists deletion queue items

### Parameters
This endpoint does not need any parameters.

### Return type

[**DeletionQueue**](../Models/DeletionQueue.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="startdeletionjob"></a>
## **StartDeletionJob**

Starts a job for the items in the deletion queue

### Parameters
This endpoint does not need any parameters.

### Return type

[**Job**](../Models/Job.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

