# DataMapper
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DataMapperId** | [**String**](string.md) | The ID of the data mapper | [optional] [default to null]
**Format** | [**String**](string.md) | The format of the dataset | [optional] [default to parquet] [enum: json, parquet]
**QueryExecutor** | [**String**](string.md) | The query executor used to query your dataset | [default to null] [enum: athena]
**Columns** | [**List**](string.md) | Columns to query for MatchIds the dataset | [default to null]
**QueryExecutorParameters** | [**DataMapper_QueryExecutorParameters**](DataMapper_QueryExecutorParameters.md) |  | [default to null]
**RoleArn** | [**String**](string.md) | Role ARN to assume when performing operations in S3 for this data mapper. The role must have the exact name &#39;S3F2DataAccessRole&#39;. | [default to null]
**DeleteOldVersions** | [**Boolean**](boolean.md) | Toggles deleting all non-latest versions of an object after a new redacted version is created | [optional] [default to true]
**IgnoreObjectNotFoundExceptions** | [**Boolean**](boolean.md) | Toggles ignoring Object Not Found errors during deletion | [optional] [default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

