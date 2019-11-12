## Monitoring the Solution

Over time you may wish to observe key metrics regarding the solution including:
- Average query execution time (Athena)
- Total data scanned (Athena)
- Job execution time (Step Functions)

To obtain these metrics you will need to take the following steps:

#### Athena
For Athena statistics, you will need to enable metrics for the workgroup you
are using to execute the queries as described [in the Athena docs](https://docs.aws.amazon.com/athena/latest/ug/query-metrics-viewing.html). 
By default the solution uses the **primary** workgroup, however you can
change this when deploying the stack.

Once you have enabled CloudWatch metrics for the workgroup, you can view the
`ProcessedBytes` and `TotalExecutionTime` metrics in CloudWatch to understand
query performance over time.

#### Step Functions
To understand how long deletion jobs are taking, view the Execution Metrics
in CloudWatch,filtering by the S3F2 state machine dimension. More information
can be found [in the Step Functions docs](https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html#cloudwatch-step-functions-execution-metrics)
