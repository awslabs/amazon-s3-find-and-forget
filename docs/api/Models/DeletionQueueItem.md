# DeletionQueueItem
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DeletionQueueItemId** | [**String**](string.md) | The Deletion Queue Item unique identifier | [default to null]
**Type** | [**String**](string.md) | MatchId Type | [default to Simple] [enum: Simple, Composite]
**MatchId** | [**oneOf&lt;string,array&gt;**](oneOf&lt;string,array&gt;.md) | The Match ID to remove from the deletion queue | [default to null]
**CreatedAt** | [**Integer**](integer.md) | Deletion queue item creation date as Epoch timestamp | [default to null]
**DataMappers** | [**List**](string.md) | The list of data mappers to apply to this Match ID | [default to null]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

