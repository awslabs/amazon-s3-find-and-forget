# JobEvent
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **String** | The Job ID | [optional] [default to null]
**CreatedAt** | **Integer** | Job creation date as Epoch timestamp | [optional] [default to null]
**EventName** | **String** | The Job Event name | [optional] [default to null]
**EventData** | [**Object**](.md) | Free form field containing data about the event. Structure varies based on the event | [optional] [default to null]
**EmitterId** | **String** | The identifier for the service or service instance which emitted the event | [optional] [default to null]
**Expires** | **Integer** | Expiry date when the item will be deleted as Epoch time | [optional] [default to null]
**Sk** | **String** | Internal field used as part of DynamoDB single table design | [optional] [default to null]
**Type** | **String** | Internal field used as part of DynamoDB single table design | [optional] [default to null] [enum: Job]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

