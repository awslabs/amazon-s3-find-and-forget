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

### Debugging FORGET_FAILED job status problems


### Debugging job status COMPLETED_WITH_ERRORS job status problems


### Debugging job status FAILED job status problems



### Debugging a job stuck in QUEUED status

If a job remains in the `QUEUED` status for more than 1 minute, there has
likely been an issue with the Jobs table stream processor which needs
resolving. 

Your system administrator may need to manually intervene to update the status
of the job in DynamoDB, or to remove the job altogether.

**WARNING:** You should only manually intervene and change a job status where
there as been a fatal error from which the system cannot be recovered, likely
due to a custom code change.

### Debugging a job stuck in RUNNING status


[User Guide]: USER_GUIDE.md
[Permissions Configuration]: USER_GUIDE.md#granting-access-to-data
[Performance Configuration]: USER_GUIDE.md#adjusting-performance-configuration