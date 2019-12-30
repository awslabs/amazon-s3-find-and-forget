import React from "react";
import { Button, Modal } from "react-bootstrap";

export default ({ show, close }) => (
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
        1. Retrieve the Athena Query Executor (<b>AthenaExecutionRoleArn</b>)
        and Delete Task (<b>DeleteTaskIAMRoleArn</b>) IAM roles from the
        Solution CloudFormation Stack Output.
      </p>
      <p>
        2. Find the relevant Bucket in the S3 AWS Web Console and{" "}
        <a href="https://s3.console.aws.amazon.com/s3/home" target="_new">
          click its name
        </a>
        , then "Permissions", and finally "Bucket Policy".
      </p>
      <p> 3. Edit the Policy and then click "Save". Here is an example:</p>
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
                    AWS: ["<AthenaExecutionRoleArn>", "<DeleteTaskIAMRoleArn>"]
                  },
                  Action: "s3:*",
                  Resource: [
                    "arn:aws:s3:::<YOUR_S3_BUCKET_NAME>",
                    "arn:aws:s3:::<YOUR_S3_BUCKET_NAME>/*"
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
