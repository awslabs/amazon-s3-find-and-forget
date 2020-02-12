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

If the problem persists, find the QueryFailed event(s) in the job history
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
updated.

- Your data is in one of the [Supported Data Formats].

### Debugging job status FAILED job status problems

- Permissions
- Timeouts/abort

### Unblocking a job stuck in QUEUED/RUNNING status

If a job remains in the QUEUED or RUNNING status for much longer than
expected, there may have been an unexpected issue with:

- The job table stream processor. [Check the logs](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-functions-logs.html)
for the stream processor Lambda to see if there have been any errors.
- One of the S3F2 state machine executions.  If there are no errors in the job
event history which indicate an issue, check the state machine execution history
for the execution with the same name as the blocked Job ID for issues.

If the state machine is still executing but in a non-recoverable state, you
can stop the execution which will trigger an Exception Job Event leading to the
job being marked as FAILED. If this doesn't resolve your issue or the execution
isn't running or your system administrator may need to manually intervene to
fix the issue, then update the job status to FAILED or remove the job and any
associated events from the Jobs table.

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
[Permissions Configuration]: USER_GUIDE.md#granting-access-to-data
[Performance Configuration]: USER_GUIDE.md#adjusting-performance-configuration
[Athena Query History]: https://docs.aws.amazon.com/athena/latest/ug/querying.html#queries-viewing-history
[Athena Troubleshooting]: https://docs.aws.amazon.com/athena/latest/ug/troubleshooting.html
[Supported Data Formats]: LIMITS.md#supported-data-formats
