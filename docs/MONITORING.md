# Monitoring

### Key Metrics

The following metrics are important indicators of the health of the Amazon S3
Find and Forget Solution:

- `AWS/SQS - ApproximateNumberOfMessagesVisible` for the Object Deletion Queue DLQ. Any
value > 0 for this metric indicates that 1 or more objects could not be
processed during a deletion job. The job which triggered the message(s) to
be put in the queue will have a status of **COMPLETED_WITH_ERRORS** and the
`ObjectUpdateFailed` event(s) will contain further debugging information.
- `AWS/SQS - ApproximateNumberOfMessagesVisible` for the Events DLQ. Any value > 0
for this metrics indicates that 1 or more Job Events could not be processed.
- `AWS/Athena - ProcessedBytes/TotalExecutionTime`. If the average processed
bytes and/or total execution time per query is rising, it may be indicative of
the average partition size also growing in size. This is not an issue per se,
however if partitions grow too large (or your dataset is unpartitioned),
you may eventually encounter Athena errors.
- `AWS/States - ExecutionsFailed`. State machine executions failing
indicates that the Amazon S3 Find and Forget solution is misconfigured error.
To resolve this, find the State Machine execution which failed and investigate
the cause of the failure.
- `AWS/States - ExecutionsTimedOut`. State machine timeouts indicate that
Amazon S3 Find and Forget is unable to complete a job before Step Functions
kills the execution due to it exceeding the allowed execution time limit. 
See [Troubleshooting] for more details.

If required, you can create CloudWatch Alarms for any of the aforementioned
metrics to be notified of potential solution misconfiguration.

### Service Level Monitoring
All standard metrics for the services used by the Amazon S3 Find and Forget
Solution are available. For detailed information about the metrics and logging
for a given service, view the relevant Monitoring docs for that service. The
key services used by the solution:

- [Lambda Metrics] / [Lambda Logging]
- [ECS Metrics] / [ECS Logging] <sup>*</sup>
- [Athena Metrics] <sup>**</sup>
- [Step Functions Metrics]
- [SQS Metrics]
- [DynamoDB Metrics]
- [S3 Metrics]

<sup>*</sup> CloudWatch Container Insights can be be enabled when deploying
the solution by setting `EnableContainerInsights` to `true`. Using Container
Insights will incur additional charges. It is disabled by default.

<sup>**</sup> To obtain Athena metrics, you will need to enable metrics for the
workgroup you are using to execute the queries as described
[in the Athena docs][Athena Metrics]. By default the solution uses the
**primary** workgroup, however you can change this when deploying the stack
using the `AthenaWorkGroup` parameter

[Lambda Metrics]: https://docs.aws.amazon.com/lambda/latest/dg/monitoring-functions-metrics.html
[Lambda Logging]: https://docs.aws.amazon.com/lambda/latest/dg/monitoring-functions-logs.html
[ECS Metrics]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/cloudwatch-metrics.html
[ECS Logging]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html#viewing_awslogs
[ECS Container Insights]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/cloudwatch-container-insights.html
[Step Functions Metrics]: https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html#cloudwatch-step-functions-execution-metrics
[Athena Metrics]: https://docs.aws.amazon.com/athena/latest/ug/query-metrics-viewing.html
[DynamoDB Metrics]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/monitoring-cloudwatch.html
[S3 Metrics]: https://docs.aws.amazon.com/AmazonS3/latest/dev/cloudwatch-monitoring.html
[SQS Metrics]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-monitoring-using-cloudwatch.html
[Troubleshooting]: ./TROUBLESHOOTING.md
