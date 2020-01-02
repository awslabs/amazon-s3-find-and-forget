import React from "react";
import { Button, Modal } from "react-bootstrap";

const { athenaExecutionRole, deleteTaskRole, region } = window.s3f2Settings;

export default ({ accountId, bucket, close, show }) => (
  <Modal centered show={show} size="lg" onHide={close}>
    <Modal.Header closeButton>
      <Modal.Title>S3 Bucket Policy</Modal.Title>
    </Modal.Header>
    <Modal.Body>
      <p>
        After configuring the data mappers, you need to configure the relevant
        S3 bucket policies to enable the solution to write and read from/to
        them.
      </p>
      <p>
        1. Open the{" "}
        <a
          href={`https://s3.console.aws.amazon.com/s3/buckets/${bucket}/?region=${region}&tab=permissions`}
          target="_new"
        >
          Permissions configuration for the {bucket} bucket in the S3 AWS Web
          Console
        </a>{" "}
        and then click "Bucket Policy".
      </p>
      <p>
        2. Edit the Policy for allowing the Athena Query Executor and Delete
        Task to read/write to the S3 Bucket and then click "Save". Here is an
        example:
      </p>
      <code>
        <pre>
          {JSON.stringify(
            {
              Version: "2012-10-17",
              Statement: [
                {
                  Sid: "AllowS3F2",
                  Effect: "Allow",
                  Principal: {
                    AWS: [
                      `arn:aws:iam::${accountId}:role/${athenaExecutionRole}`,
                      `arn:aws:iam::${accountId}:role/${deleteTaskRole}`
                    ]
                  },
                  Action: "s3:*",
                  Resource: [
                    `arn:aws:s3:::${bucket}`,
                    `arn:aws:s3:::${bucket}/*`
                  ]
                }
              ]
            },
            null,
            2
          )}
        </pre>
      </code>
    </Modal.Body>
    <Modal.Footer>
      <Button className="aws-button action-button" onClick={close}>
        Close
      </Button>
    </Modal.Footer>
  </Modal>
);
