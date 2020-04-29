# JobSummary
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | [**String**](string.md) | The Job ID | [default to null]
**JobStatus** | [**String**](string.md) | The Job status. When a job is first created, it will remain in queued till the workflow starts | [default to QUEUED] [enum: QUEUED, RUNNING, FORGET_COMPLETED_CLEANUP_IN_PROGRESS, COMPLETED, COMPLETED_CLEANUP_FAILED, FAILED, FIND_FAILED, FORGET_FAILED, FORGET_PARTIALLY_FAILED]
**CreatedAt** | [**Integer**](integer.md) | Job creation date as Epoch timestamp | [default to null]
**JobStartTime** | [**Integer**](integer.md) | Job start date as Epoch timestamp | [optional] [default to null]
**JobFinishTime** | [**Integer**](integer.md) | Job finish date as Epoch timestamp | [optional] [default to null]
**TotalObjectUpdatedCount** | [**Integer**](integer.md) | Total number of successfully updated objects | [optional] [default to 0]
**TotalObjectUpdateFailedCount** | [**Integer**](integer.md) | Total number of objects which could not be successfully updated | [optional] [default to 0]
**TotalObjectRollbackFailedCount** | [**Integer**](integer.md) | Total number of objects which could not be successfully rolled back after detecting an integrity conflict | [optional] [default to 0]
**TotalQueryCount** | [**Integer**](integer.md) | Total number of queries executed during the find phase | [optional] [default to 0]
**TotalQueryFailedCount** | [**Integer**](integer.md) | Total number of unsuccessfully executed queries during the find phase | [optional] [default to 0]
**TotalQueryScannedInBytes** | [**Integer**](integer.md) | Total amount of data scanned during the find phase | [optional] [default to 0]
**TotalQuerySucceededCount** | [**Integer**](integer.md) | Total number of successfully executed queries during the find phase | [optional] [default to 0]
**TotalQueryTimeInMillis** | [**Integer**](integer.md) | Total time spent by the query executor for this job | [optional] [default to 0]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

