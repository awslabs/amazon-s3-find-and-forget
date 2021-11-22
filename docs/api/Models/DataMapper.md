# DataMapper
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DataMapperId** | **String** | The ID of the data mapper | [optional] [default to null]
**Format** | **String** | The format of the dataset | [optional] [default to parquet] [enum: json, parquet]
**QueryExecutor** | **String** | The query executor used to query your dataset | [default to null] [enum: athena]
**Columns** | **List** | Columns to query for MatchIds the dataset | [default to null]
**QueryExecutorParameters** | [**DataMapper_QueryExecutorParameters**](DataMapper_QueryExecutorParameters.md) |  | [default to null]
**RoleArn** | **String** | Role ARN to assume when performing operations in S3 for this data mapper. The role must have the exact name &#39;S3F2DataAccessRole&#39;. | [default to null]
**DeleteOldVersions** | **Boolean** | Toggles deleting all non-latest versions of an object after a new redacted version is created | [optional] [default to true]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

