# Upgrade Guide

## Migrating from <=v0.24 to v0.25

Prior to v0.25, the Deletion Queue was synchronously processed on Job Creation
and stored in DynamoDB. As a result, the job API provided the full queue for a
given job in the `DeletionQueueItems` property and there was a limit of ~375KB
on the queue size for each individual Job. If the size for a given job would
have exceeded the allowed space, the `DeletionQueueItemsSkipped` property would
have been set to `true` and it would have been necessary to run one or more
deletion jobs, upon completion, to process the whole queue.

Starting from v0.25, the queue is processed asynchronously after job creation
and is stored in S3 in order to remove the queue limit. As a result:

1. The fields `DeletionQueueItemsSkipped` and `DeletionQueueItems` are both
   removed from the `GET /jobs/{job_id}` and `DELETE /queue` APIs.
2. A new Job Event is created when the Query Planning ends called
   `QueryPlanningComplete`, containing details of the query planning phase.
3. After Query Planning, the `QueryPlanningComplete` event's payload is
   available in the `GET /jobs/{job_id}` API for a quick lookup of the
   properties:
   - `GeneratedQueries` is the number of queries planned for execution
   - `DeletionQueueSize` is the size of the queue for the Job
   - `Manifests` is an array of S3 Objects containing the location for the Job
     manifests. There is a manifest for each combination of `JobId` and
     `DataMapperId`, and each manifest contains the full queue including the
     MatchIds.
4. The manifests follow the same expiration policy as the Job Details (they will
   get automatically removed if the `JobDetailsRetentionDays` parameter is
   configured when installing the solution).
5. If you relied on the removed `DeletionQueueItems` parameter to inspect the
   Job's queue, you'll need to migrate to fetching the S3 Manifests or querying
   the AWS Glue Manifests Table.
6. The deletion queue items are not visible in the UI anymore in the job details
   page and in the job JSON export.

## Migrating from <=v0.8 to v0.9

The default behaviour of the solution has been changed in v0.9 to deploy and use
a purpose-built VPC when creating the solution CloudFormation stack.

If you have deployed the standalone VPC stack provided in previous versions, you
should should set `DeployVpc` to **true** when upgrading to v0.9 and input the
same values for the `FlowLogsGroup` and `FlowLogsRoleArn` parameters that were
used when deploying the standalone VPC stack. After the deployment of v0.9 is
complete, you should delete the old VPC stack.

To continue using an existing VPC, you must set `DeployVpc` to **false** when
upgrading to v0.9.
