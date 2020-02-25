# ./Models.JobEvent
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | [**String**](string.md) | The Job ID | [optional] [default to null]
**CreatedAt** | [**Integer**](integer.md) | Job creation date as Epoch timestamp | [optional] [default to null]
**EventName** | [**String**](string.md) | The Job Event name | [optional] [default to null]
**EventData** | [**Object**](.md) | Free form field containing data about the event. Structure varies based on the event | [optional] [default to null]
**EmitterId** | [**String**](string.md) | The identifier for the service or service instance which emitted the event | [optional] [default to null]
**Expires** | [**Integer**](integer.md) | Expiry date when the item will be deleted as Epoch time | [optional] [default to null]
**Sk** | [**String**](string.md) | Internal field used as part of DynamoDB single table design | [optional] [default to null]
**Type** | [**String**](string.md) | Internal field used as part of DynamoDB single table design | [optional] [default to null]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

