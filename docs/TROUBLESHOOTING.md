# Troubleshooting

This section outlines steps to assist you with resolving issues
deploying, configuring and using the Amazon S3 Find and Forget solution.

If you're unable to resolve an issue using this information you can [report the
issue on GitHub](../CONTRIBUTING.md#reporting-bugsfeature-requests).

### Expected Results Not Found

If the Find phase does not identify the expected objects for the matches in the
deletion queue, verify the following:

- You have chosen the relevant data mappers for the matches in the deletion
  queue.
- Your data mappers are referencing the correct S3 locations.
- Your data mappers have been configured to search the correct columns.
- All partitions have been loaded into the Glue Data Catalog.

### Job appears stuck in QUEUED/RUNNING status

If a job remains in a QUEUED or RUNNING status for much longer than expected,
there may be an issue relating to:

- AWS Fargate accessing the ECR service endpoint. Enabling the required network
  access from the subnets/security groups in which Forget Fargate tasks are
  launched will unblock the job without requiring manual intervention. For more
  information see [VPC Configuration] in the [User Guide].
- Errors in job table stream processor. [Check the logs](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-functions-logs.html)
  of the stream processor Lambda function for errors.
- Unhandled state machine execution errors. If there are no errors in the job
  event history which indicate an issue, check the state machine execution
  history of the execution with the same name as the blocked job ID.
- The containers have exausted memory or vCPUs capacity while processing large
  (+GB size) files. See also [Service Level Monitoring](MONITORING.md#service-level-monitoring).

If the state machine is still executing but in a non-recoverable state, you
can stop the state machine execution manually which will trigger an Exception
job event — the job will enter a `FAILED` status. 

If this doesn't resolve the issue or the execution isn't running, you can
manually update the job status to FAILED or remove the job and any associated
events from the Jobs table<sup>*</sup>.

<sup>*</sup> **WARNING:** You should manually intervene only when there as been
a fatal error from which the system cannot recover.

### Job status: COMPLETED_CLEANUP_FAILED

A `COMPLETED_CLEANUP_FAILED` status indicates that the job has completed, but
an error occurred when removing the processed matches from the deletion queue.

Some possible causes for this are:

- The stream processor Lambda function does not have permissions to manipulate
  the DynamoDB table.
- The item has been manually removed from the deletion queue table via a direct
  call to the DynamoDB API.

You can find more details of the cause by checking the job event history for a
**CleanupFailed** event, then viewing the event data.

As the processed matches will still be on the queue, you can choose to either:

- Manually remove the processed matches via the solution web interface or APIs.
- Take no action — the matches will remain in the queue and be re-processed
  during the next deletion job run.

### Job status: FAILED

A `FAILED` status indicates that the job has terminated due to a generic
exception. 

Some possible causes for this are:

- One of the tasks in the main step function failed.
- There was a permissions issue encountered in one of the solution components.
- The state machine execution time has timed out, or has exceeded the service
  quota for state machine execution history.

To find information on what caused the failure, check the deletion job log for
an **Exception** event and inspect that event's event data.

Errors relating to Step Functions such as timeouts or exceeding the permitted
execution history length, may be resolvable by increasing the waiter
configuration as described in [Performance Configuration].

### Job status: FIND_FAILED

A `FIND_FAILED` status indicates that the job has terminated because one or
more data mapper queries failed to execute.

If you are using Athena and Glue as data mappers, you should first verify the
following:

- You have granted permissions to the Athena IAM role for access to the S3
  buckets referenced by your data mappers **and** any AWS KMS keys used to
  encrypt the S3 objects. For more information see [Permissions Configuration]
  in the [User Guide].
- The concurrency setting for the solution does not exceed the limits for
  concurrent Athena queries for your AWS account or the Athena workgroup the
  solution is configured to use.  For more information see [Performance
  Configuration] in the [User Guide].
- Your data is compatible within the [solution limits].

If you made any changes whilst verifying the prior points, you should attempt
to run a new deletion job.

To find further details of the cause of the failure you should inspect the
deletion job log and inspect the event data for any **QueryFailed** events. 

Athena queries may fail if the length of a query sent to Athena exceed the
Athena query string length limit (see [Athena Service Quotas]). If queries are
failing for this reason, you will need to reduce the number of matches queued
when running a deletion job.

To troubleshoot Athena queries further, find the `QueryId` from the event data
and match this to the query in the [Athena Query History]. You can use the
[Athena Troubleshooting] guide for Athena troubleshooting steps.

### Job status: FORGET_FAILED

A `FORGET_FAILED` status indicates that the job has terminated because a fatal
error occurred during the _forget_ phase of the job. S3 objects _may_ have
been modified.

Check the job log for a **ForgetPhaseFailed** event. Examining the event data
for this event will provide you with more information about the underlying
cause of the failure.

### Job status: FORGET_PARTIALLY_FAILED

A `FORGET_PARTIALLY_FAILED` status indicates that the job has completed, but
that the _forget_ phase was unable to process one or more objects.

Each object that was not correctly processed will result in a message sent to
the object dead letter queue ("DLQ"; see `DLQUrl` in the CloudFormation stack
outputs) and an **ObjectUpdateFailed** event in the job event history containing
error information. Check the content of any **ObjectUpdateFailed** events to
ascertain the root cause of an issue.

Verify the following:

- No processes are writing new version of existing objects while a job is running.
  When the system writes a new version of a object, an integrity check is performed
  to verify that during processing, no new versions of an object were created and that a
  delete marker for the object was not created. If either case is detected, an
  **ObjectUpdateFailed** event will be present in the job event history..
- You have granted permissions to the Fargate task IAM role for access to the
  S3 buckets referenced by your data mappers **and** any AWS KMS keys used to
  encrypt the data. For more information see [Permissions Configuration] in the
  [User Guide].
- You have configured the VPC used for the Fargate tasks according to the 
  [VPC Configuration] section.
- Your data is compatible within the [solution limits].
- Your data is not corrupted.

To reprocess the objects, populate the deletion queue with the same matches
and run a new deletion job.


[User Guide]: USER_GUIDE.md
[VPC Configuration]: USER_GUIDE.md#pre-requisite-configuring-a-vpc-for-the-solution
[Permissions Configuration]: USER_GUIDE.md#granting-access-to-data
[Performance Configuration]: USER_GUIDE.md#adjusting-performance-configuration
[Athena Service Quotas]: https://docs.aws.amazon.com/athena/latest/ug/service-limits.html
[Athena Query History]: https://docs.aws.amazon.com/athena/latest/ug/querying.html#queries-viewing-history
[Athena Troubleshooting]: https://docs.aws.amazon.com/athena/latest/ug/troubleshooting.html
[solution limits]: LIMITS.md
[CloudWatch Container Insights]: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/ContainerInsights.html
