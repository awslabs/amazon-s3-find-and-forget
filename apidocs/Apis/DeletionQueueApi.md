# DeletionQueueApi

All URIs are relative to *https://&lt;your-api-gw-url&gt;/Prod*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addToDeletionQueue**](DeletionQueueApi.md#addToDeletionQueue) | **PATCH** /queue | Adds an item to the deletion queue
[**deleteMatches**](DeletionQueueApi.md#deleteMatches) | **DELETE** /queue/matches | Removes an item from the deletion queue
[**listDeletionQueueMatches**](DeletionQueueApi.md#listDeletionQueueMatches) | **GET** /queue | Lists deletion queue items
[**startDeletionJob**](DeletionQueueApi.md#startDeletionJob) | **DELETE** /queue | Starts a job for the items in the deletion queue




<a name="addToDeletionQueue"></a>
## **addToDeletionQueue**
> DeletionQueueItem addToDeletionQueue(createDeletionQueueItem)

Adds an item to the deletion queue

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **createDeletionQueueItem** | [**CreateDeletionQueueItem**](../Models/CreateDeletionQueueItem.md)| Request body containing details of the Match to add to the Deletion Queue |


### Return type

[**DeletionQueueItem**](../Models/DeletionQueueItem.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json, 
- **Accept**: application/json, 


<a name="deleteMatches"></a>
## **deleteMatches**
> deleteMatches(listOfMatchDeletions)

Removes an item from the deletion queue

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **listOfMatchDeletions** | [**ListOfMatchDeletions**](../Models/ListOfMatchDeletions.md)|  |


### Return type



### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: application/json, 
- **Accept**: 


<a name="listDeletionQueueMatches"></a>
## **listDeletionQueueMatches**
> DeletionQueue listDeletionQueueMatches()

Lists deletion queue items

### Parameters
This endpoint does not need any parameters.


### Return type

[**DeletionQueue**](../Models/DeletionQueue.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: application/json, 


<a name="startDeletionJob"></a>
## **startDeletionJob**
> Job startDeletionJob()

Starts a job for the items in the deletion queue

### Parameters
This endpoint does not need any parameters.


### Return type

[**Job**](../Models/Job.md)

### Authorization

[CognitoAuthorizer](../README.md#CognitoAuthorizer)

### HTTP request headers

- **Content-Type**: 
- **Accept**: application/json


