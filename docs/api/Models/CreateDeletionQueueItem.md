# CreateDeletionQueueItem
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Type** | **String** | MatchId Type | [optional] [default to Simple] [enum: Simple, Composite]
**MatchId** | [**oneOf&lt;string,array&gt;**](oneOf&lt;string,array&gt;.md) | The Match ID to remove from the deletion queue | [default to null]
**DataMappers** | **List** | The list of data mappers to apply to this Match ID | [optional] [default to ["*"]]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

