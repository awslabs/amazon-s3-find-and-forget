# DataMapper
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DataMapperId** | [**String**](string.md) | The ID of the data mapper | [optional] [default to null]
**Format** | [**String**](string.md) | The format of the dataset | [optional] [default to parquet] [enum: parquet]
**QueryExecutor** | [**String**](string.md) | The query executor used to query your dataset | [default to null] [enum: athena]
**Columns** | [**List**](string.md) | Columns to query for MatchIds the dataset | [default to null]
**QueryExecutorParameters** | [**DataMapper_QueryExecutorParameters**](DataMapper_QueryExecutorParameters.md) |  | [default to null]
**RoleArn** | [**String**](string.md) | Role ARN to assume when performing operations in S3 for this data mapper. The role must have the exact name &#39;S3F2DataAccessRole&#39;. | [default to null]
**DeleteOldVersions** | [**Boolean**](boolean.md) | Whether to delete old versions of objects after writing the newly redacted version | [optional] [default to false]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

