# Job
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**false** | [**String**](string.md) | The Job ID | [default to null]
**false** | [**String**](string.md) | The Job status. When a job is first created, it will remain in queued till the workflow starts | [default to QUEUED]
**false** | [**Integer**](integer.md) | Job creation date as Epoch timestamp | [default to null]
**false** | [**Integer**](integer.md) | Job start date as Epoch timestamp | [optional] [default to null]
**false** | [**Integer**](integer.md) | Job finish date as Epoch timestamp | [optional] [default to null]
**false** | [**List**](string.md) | The deletion queue items applied to this job | [default to null]
**false** | [**Integer**](integer.md) | Athena concurrency setting for this job | [default to null]
**false** | [**Integer**](integer.md) | Max Fargate tasks setting for this job | [default to null]
**false** | [**Integer**](integer.md) | Forget queue wait setting for this job | [default to null]
**false** | [**Integer**](integer.md) | Query execution wait setting for this job | [default to null]
**false** | [**Integer**](integer.md) | Query queue worker wait setting for this job | [default to null]
**false** | [**Boolean**](boolean.md) | Safe Mode setting for this job | [default to true]
**false** | [**Integer**](integer.md) | Total number of successfully updated objects | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Total number of objects which could not be successfully updated | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Total number of queries executed during the find phase | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Total number of unsuccessfully executed queries during the find phase | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Total amount of data scanned during the find phase | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Total number of successfully executed queries during the find phase | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Total time spent by the query executor for this job | [optional] [default to 0]
**false** | [**Integer**](integer.md) | Expiry date when the item will be deleted as Epoch time | [optional] [default to null]
**false** | [**String**](string.md) | Internal field used as part of DynamoDB single table design | [default to null]
**false** | [**String**](string.md) | Internal field used as part of DynamoDB single table design | [default to null]
**false** | [**String**](string.md) | Internal field used as part of DynamoDB single table design | [default to null]


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

    
