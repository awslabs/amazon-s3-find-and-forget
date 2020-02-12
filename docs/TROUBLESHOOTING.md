# Troubleshooting

This section outlines what steps to take in order to resolve issues with
deploying, configuring and using the Amazon S3 Find and Forget solution.

### Debugging FIND_FAILED job status problems

If your job finishes with a status of FIND_FAILED it indicates that
one or more queries failed to execute. When using Athena + Glue for your
data mappers, verify the following:

- You have granted the Athena role access to the buckets referenced by your
data mappers **and** any CMKs used to encrypt the data. For more information
see [Permissions Configuration] in the [User Guide].
- Your concurrency settings does not exceed your account limit or the limit
placed on the Athena workgroup you have configured the solution to use.
For more information see [Performance Configuration] in the [User Guide].
- Your data is in one of the [Supported Data Formats].
- The total size of the queries being ran by Athena does not exceed the
maximum Athena query string length outlined in [Athena Service Quotas]. If
queries are failing for this reason, you will need to reduce the number of
matches being ran per deletion job.
- Your data is compatible with the limits described in the solution [Limits].

If the problem persists, find the **QueryFailed** event(s) in the job history
to find out more specific details of the error. If you need more information,
extract the `QueryId` from the event data, find the query in the
[Athena Query History] and use the [Athena Troubleshooting] guide to
debug the issue further.

### Debugging FORGET_FAILED job status problems

If your job finishes with a status of FORGET_FAILED it indicates that one the
solution was unable to perform the Forget phase. To debug the issue, check the
event data of the **ForgetPhaseFailed**` event to find out more information
about what error caused the issue.

**Important:** This status does **not** indicate that there was an issue
updating specific objects, but rather that the Forget phase as a whole was
unable to run successfully. 

### Debugging job status FORGET_PARTIALLY_FAILED job status problems

If your job finishes with a status of FORGET_PARTIALLY_FAILED it indicates that
one or more objects that were found during the Find phase were unable to be
updated. Verify the following:

- You have granted the Fargate role access to the buckets referenced by your
data mappers **and** any CMKs used to encrypt the data. For more information
see [Permissions Configuration] in the [User Guide].
- Your data is in one of the [Supported Data Formats].
- Your data is compatible with the limits described in the solution [Limits].
- Your data is not corrupted
- Your network is configuration allows access to the relevant AWS services as
described in [VPC Configuration]

For each object which was unable to be processed successfully, a message
will be placed on the objects DLQ (see `DLQUrl` in the CloudFormation stack
outputs) and an **ObjectUpdateFailed** event containing detailed error
information will be present in the job event history. To reprocess these
objects you will need to run a new job with the same Matches in the Deletion
Queue.

### Debugging job status FAILED problems

If your job finishes with a status of FAILED it indicates that there was
an unhandled exception during the job execution. Possible causes are:

- One of the tasks in the main step function failed for an unknown reason.
- There was a permissions issue for one of the solution components.
- The state machine exceeded timed out or exceeded the service quota for
state machine execution history.

For more information on what caused the issue, check the event data for
the **Exception** in event in the job event history. If there error is related
to Step Functions service quotas such as timeouts or exceeded the permitted
execution history length, you may be able to resolve this by increasing the
waiter configuration as described in [Performance Configuration]

### Debugging job status COMPLETED_CLEANUP_FAILED problems

If your job finishes with a status of COMPLETED_CLEANUP_FAILED it indicates
that although the Find and Forget phases completed successfully, the job was
unable to remove the items it has deleted from the Deletion Queue. This is
most likely due to either the permissions of the stream processor Lambda being
changed or an item being manually removed from the Deletion Queue table via
a direct call to the DynamoDB API. Check the **CleanupFailed** event in the job
event history for more information.

To cleanup the Deletion Queue, either delete the items from the solution UI,
or leave them in the queue and allow them to be removed by the subsequent job
execution.

### Unblocking a job stuck in QUEUED/RUNNING status

If a job remains in the QUEUED or RUNNING status for much longer than
expected, there may have been an unexpected issue with:

- AWS Fargate accessing the ECR service endpoint. Enabling the required network
from the subnets/security groups in which Forget Fargate tasks are launched
will unblock the job without requiring manual intervention. For more
information see [VPC Configuration] in the [User Guide].
- The job table stream processor. [Check the logs](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-functions-logs.html)
for the stream processor Lambda to see if there have been any errors.
- One of the S3F2 state machine executions. If there are no errors in the job
event history which indicate an issue, check the state machine execution history
for the execution with the same name as the blocked Job ID for issues.

If the state machine is still executing but in a non-recoverable state, you
can stop the execution which will trigger an Exception Job Event leading to the
job being marked as FAILED. If this doesn't resolve your issue or the execution
isn't running, you can manually update the job status to FAILED or remove the
job and any associated events from the Jobs table.

**WARNING:** You should only manually intervene where there as been a fatal
error from which the system cannot recover.

### Expected Results Not Found

If the Find phase does not identify the expected files for the matches in the
deletion queue, verify the following:

- You have chosen the relevant data mappers for the matches in the deletion
  queue.
- Your data mappers are referencing the correct S3 locations
- Your data mappers are configured to search the correct columns
- All partitions have been loaded into the Glue Data Catalog

[User Guide]: USER_GUIDE.md
[VPC Configuration]: USER_GUIDE.md#pre-requisite-configuring-a-vpc-for-the-solution
[Permissions Configuration]: USER_GUIDE.md#granting-access-to-data
[Performance Configuration]: USER_GUIDE.md#adjusting-performance-configuration
[Athena Service Quotas]: https://docs.aws.amazon.com/athena/latest/ug/service-limits.html
[Athena Query History]: https://docs.aws.amazon.com/athena/latest/ug/querying.html#queries-viewing-history
[Athena Troubleshooting]: https://docs.aws.amazon.com/athena/latest/ug/troubleshooting.html
[Supported Data Formats]: LIMITS.md#supported-data-formats
[Limits]: LIMITS.md
