# Job
## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **String** | The Job ID | [default to null]
**JobStatus** | **String** | The Job status. When a job is first created, it will remain in queued till the workflow starts | [default to QUEUED] [enum: QUEUED, RUNNING, FORGET_COMPLETED_CLEANUP_IN_PROGRESS, COMPLETED, COMPLETED_CLEANUP_FAILED, FAILED, FIND_FAILED, FORGET_FAILED, FORGET_PARTIALLY_FAILED]
**CreatedAt** | **Integer** | Job creation date as Epoch timestamp | [default to null]
**JobStartTime** | **Integer** | Job start date as Epoch timestamp | [optional] [default to null]
**JobFinishTime** | **Integer** | Job finish date as Epoch timestamp | [optional] [default to null]
**AthenaConcurrencyLimit** | **Integer** | Athena concurrency setting for this job | [default to null]
**AthenaQueryMaxRetries** | **Integer** | Max number of retries to each Athena query after a failure | [default to null]
**DeletionTasksMaxNumber** | **Integer** | Max Fargate tasks setting for this job | [default to null]
**ForgetQueueWaitSeconds** | **Integer** | Forget queue wait setting for this job | [default to null]
**QueryExecutionWaitSeconds** | **Integer** | Query execution wait setting for this job | [default to null]
**QueryQueueWaitSeconds** | **Integer** | Query queue worker wait setting for this job | [default to null]
**TotalObjectUpdatedCount** | **Integer** | Total number of successfully updated objects | [optional] [default to 0]
**TotalObjectUpdateFailedCount** | **Integer** | Total number of objects which could not be successfully updated | [optional] [default to 0]
**TotalObjectRollbackFailedCount** | **Integer** | Total number of objects which could not be successfully rolled back after detecting an integrity conflict | [optional] [default to 0]
**TotalQueryCount** | **Integer** | Total number of queries executed during the find phase | [optional] [default to 0]
**TotalQueryFailedCount** | **Integer** | Total number of unsuccessfully executed queries during the find phase | [optional] [default to 0]
**TotalQueryScannedInBytes** | **Integer** | Total amount of data scanned during the find phase | [optional] [default to 0]
**TotalQuerySucceededCount** | **Integer** | Total number of successfully executed queries during the find phase | [optional] [default to 0]
**TotalQueryTimeInMillis** | **Integer** | Total time spent by the query executor for this job | [optional] [default to 0]
**Expires** | **Integer** | Expiry date when the item will be deleted as Epoch time | [optional] [default to null]
**Sk** | **String** | Internal field used as part of DynamoDB single table design | [default to null]
**Type** | **String** | Internal field used as part of DynamoDB single table design | [default to null] [enum: Job]
**GSIBucket** | **String** | Internal field used as part of DynamoDB single table design | [default to null]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

