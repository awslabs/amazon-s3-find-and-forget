# Production Readiness Guidelines

Conducting your own testing prior to operating in production is important for
various reasons, including gaining extra confidence against unwanted data loss
due to misconfiguration and uncontrolled spending.

## 1. Review the Solution Limits

Before starting, consult the [Limits] documentation to ensure your datasets are
in a supported format and your buckets' configuration is compatible with the
solution requirements.

## 2. Learn about costs

Consult the [Cost Overview guide] to learn about cost and start having an idea
of the cost of deletion.

## 3. Deploy the solution in a test environment

We recommend starting to evaluate the solution by deploying it in a test account
with a sample of your dataset. After configuring the solution, identify a set of
queries to run on your dataset before and after [running a Deletion Job].

> **Note:** You don't need to have a full copy of each dataset, but we recommend
> to have at least the same schema in order to make sure the test queries are as
> close to production as possible.

These are examples of test queries:

- Count the total number of rows in a dataset (A)
- Count the number of rows that need to be deleted from the same dataset (B)
- Run a query to fetch one or more rows that won't be affected by deletion but
  contained in an object that will be rewritten because of other rows (C)

After running a deletion job:

- Repeat the first 2 queries to make sure the row count is correct:
  A<sub>1</sub>=A<sub>0</sub>-B<sub>0</sub> and B<sub>1</sub>=0
- Repeat the third query to ensure the rows have been re-written without
  affecting their schema (for instance, there is no unwanted type coercion
  against `date` or `number` types): C<sub>1</sub>=C<sub>0</sub>

If any error occurs or data doesn't match, please review the [troubleshooting
guide] or check for [existing issues]. If you cannot find a resolution, feel
free to [open an issue].

## 4. Identify your own extra requirements

This guidelines are provided as suggested steps to identify your own acceptance
criteria, but they are not intended to be a comprehensive list. Please identify
any other extra step before moving to production. If you have any question
please [open an issue]. We appreciate your feedback.

[cost overview guide]: COST_OVERVIEW.md
[existing issues]: https://github.com/awslabs/amazon-s3-find-and-forget/issues
[limits]: LIMITS.md
[open an issue]: https://github.com/awslabs/amazon-s3-find-and-forget/issues
[running a deletion job]: USER_GUIDE.md#running-a-deletion-job
[troubleshooting guide]: TROUBLESHOOTING.md
