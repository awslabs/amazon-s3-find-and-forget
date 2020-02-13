import React, { useState } from "react";
import { Button, Modal, Tabs, Tab } from "react-bootstrap";
import Icon from "./Icon";

const { athenaExecutionRole, deleteTaskRole, region } = window.s3f2Settings;

export default ({ accountId, bucket, close, show, location }) => {
  const [key, setKey] = useState('bucket');
  const tabs = [{
    key: "bucket",
    title: "Bucket Access",
    content: <BucketPolicy bucket={bucket} accountId={accountId} location={location} />
  }, {
    key: "kms",
    title: "KMS Access",
    content: <KmsPolicy bucket={bucket} accountId={accountId} />
  }, {
    key: "ca",
    title: "Cross Account Access",
    content: <CrossAccountPolicy bucket={bucket} accountId={accountId} location={location} />
  }]

  return (
    <Modal centered show={show} size="lg" onHide={close}>
      <Modal.Header closeButton>
        <Modal.Title>Generate Policies</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <p>
          After configuring a data mapper, you need to configure the relevant
          S3 bucket, CMK and IAM policies to enable the solution to read and write
          from/to the bucket. The policy statements provided below are examples of how
          to grant the required access.
        </p>
        <Tabs activeKey={key} onSelect={k => setKey(k)}>
          {
            tabs.map(tab => <Tab key={tab.key} eventKey={tab.key} title={<span>{tab.title}</span>}>
              {tab.content}
            </Tab>)
          }
        </Tabs>
      </Modal.Body>
      <Modal.Footer>
        <Button className="aws-button action-button" onClick={close}>
          Close
        </Button>
      </Modal.Footer>
    </Modal>
  )
};

const BucketPolicy = ({ bucket, accountId, location }) => (<>
  <ol>
    <li>
      <p>
          Open the{" "}
        <a
          href={`https://s3.console.aws.amazon.com/s3/buckets/${bucket}/?region=${region}&tab=permissions`}
          target="_new"
        >
          Bucket Permissions configuration in the S3 AWS Web Console
        </a>{" "}
        and then choose <strong>Bucket Policy</strong>.
      </p>
    </li>
    <li>
      <p>
        Edit the Policy to grant the Athena Query Executor and Delete
        Task IAM roles read/write access to the S3 Bucket and then choose
        <strong> Save</strong>. The following is an example bucket policy:
      </p>
      <code>
        <pre>
          {JSON.stringify(
            {
              Version: "2012-10-17",
              Statement: [
                {
                  Sid: "AllowS3F2Read",
                  Effect: "Allow",
                  Principal: {
                    AWS: [
                      `arn:aws:iam::${accountId}:role/${athenaExecutionRole}`,
                      `arn:aws:iam::${accountId}:role/${deleteTaskRole}`
                    ]
                  },
                  Action: [
                    "s3:GetBucketLocation",
                    "s3:GetObject*",
                    "s3:ListBucket"
                  ],
                  Resource: [
                    `arn:aws:s3:::${bucket}`,
                    `arn:aws:s3:::${location.replace("s3://", "")}*`
                  ]
                }, {
                  Sid: "AllowS3F2Write",
                  Effect: "Allow",
                  Principal: {
                    AWS: [
                      `arn:aws:iam::${accountId}:role/${deleteTaskRole}`
                    ]
                  },
                  Action: [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketRequestPayment",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObject*"
                  ],
                  Resource: [
                    `arn:aws:s3:::${bucket}`,
                    `arn:aws:s3:::${location.replace("s3://", "")}*`
                  ]
                }
              ]
            },
            null,
            2
          )}
        </pre>
      </code>
    </li>
  </ol>
</>)


const KmsPolicy = ({ bucket, accountId }) => (<>
  <p>
    <Icon type="alert-info" />
    <strong> If your bucket is not using a CMK, you can skip this step</strong>
  </p>
  <ol>
    <li>
      <p>
        Open the{" "}
        <a
          href={`https://console.aws.amazon.com/kms/home?region=${region}#/kms/keys`}
          target="_new"
        >
          KMS console
        </a>{" "}
        and then choose <strong>Key ID</strong> for the key used to encrypt objects in
        the {bucket} bucket.
      </p>
    </li>
    <li>
      <p>
        Update the Key Policy to add the Athena Query Executor and Delete Task IAM
        roles as <strong>Key users</strong>. If using the policy view, here are example
        statements you can add to the key policy to grant the required access:
      </p>
      <code>
        <pre>
          {JSON.stringify(
            [{
              "Sid": "AllowS3F2Use",
              "Effect": "Allow",
              "Principal": {
                AWS: [
                  `arn:aws:iam::${accountId}:role/${athenaExecutionRole}`,
                  `arn:aws:iam::${accountId}:role/${deleteTaskRole}`
                ]
              },
              "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:DescribeKey"
              ],
              "Resource": "*"
            }, {
              "Sid": "AllowS3F2Grants",
              "Effect": "Allow",
              "Principal": {
                AWS: [
                  `arn:aws:iam::${accountId}:role/${athenaExecutionRole}`,
                  `arn:aws:iam::${accountId}:role/${deleteTaskRole}`
                ]
              },
              "Action": [
                "kms:CreateGrant",
                "kms:ListGrants",
                "kms:RevokeGrant"
              ],
              "Resource": "*",
              "Condition": {
                "Bool": {
                  "kms:GrantIsForAWSResource": "true"
                }
              }
            }],
            null,
            2
          )}
        </pre>
      </code>
    </li>
  </ol>
</>)

const CrossAccountPolicy = ({ bucket, accountId, location }) => (<>
  <p>
    <Icon type="alert-info" />
    <strong> If your bucket and KMS key used for encrypting objects are owned by the
      account {accountId}, you can skip this step</strong>
  </p>
  <ol>
    <li>
      <p>
        Open the{" "}
        <a
          href={`https://console.aws.amazon.com/iam/home?region=${region}#/roles/${athenaExecutionRole.split('/').pop()}`}
          target="_new"
        >
          Athena Query Executor role in the IAM console
        </a>{" "}
        and then choose <strong>Add inline policy</strong> then choose the <strong>JSON</strong> tab.
      </p>
    </li>
    <li>
      <p>
        Add a Policy to grant the Athena Query Executor read access to the S3 Bucket
        then choose <strong>Review Policy</strong>. Input a name for the policy and
        choose <strong>Create Policy</strong>. An example IAM policy is provided below.
      </p>
      <p>
        <strong>Note:</strong> you only need to include the AllowS3F2Use and AllowS3F2Grants
        if you are using a CMK to encrypt objects in the bucket. If so, replace
        <strong> &lt;your-cmk-arn&gt;</strong> with the ARN of your CMK.
      </p>
      <code>
        <pre>
          {JSON.stringify(
            {
              Version: "2012-10-17",
              Statement: [
                {
                  Sid: "AllowS3F2Read",
                  Effect: "Allow",
                  Action: [
                    "s3:GetBucketLocation",
                    "s3:GetObject*",
                    "s3:ListBucket"
                  ],
                  Resource: [
                    `arn:aws:s3:::${bucket}`,
                    `arn:aws:s3:::${location.replace("s3://", "")}*`
                  ]
                },
                {
                  "Sid": "AllowS3F2Use",
                  "Effect": "Allow",
                  "Action": [
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey"
                  ],
                  "Resource": "<your-cmk-arn>"
                }, 
                {
                  "Sid": "AllowS3F2Grants",
                  "Effect": "Allow",
                  "Action": [
                    "kms:CreateGrant",
                    "kms:ListGrants",
                    "kms:RevokeGrant"
                  ],
                  "Resource": "<your-cmk-arn>",
                  "Condition": {
                    "Bool": {
                      "kms:GrantIsForAWSResource": "true"
                    }
                  }
                }
              ]
            },
            null,
            2
          )}
        </pre>
      </code>
    </li>
    <li>
      <p>
        Open the{" "}
        <a
          href={`https://console.aws.amazon.com/iam/home?region=${region}#/roles/${deleteTaskRole.split('/').pop()}`}
          target="_new"
        >
          Deletion Task role in the IAM console
        </a>{" "}
        and then choose <strong>Add inline policy</strong> then choose the <strong>JSON</strong> tab.
      </p>
    </li>
    <li>
      <p>
        Add a Policy to grant the Deletion Task read/write access to the S3 Bucket
        then choose <strong>Review Policy</strong>. Input a name for the policy and
        choose <strong>Create Policy</strong>. An example IAM policy is provided below.
      </p>
      <p>
        <strong>Note:</strong> you only need to include the AllowS3F2Use and AllowS3F2Grants
        if you are using a CMK to encrypt objects in the bucket. If so, replace
        <strong> &lt;your-cmk-arn&gt;</strong> with the ARN of your CMK.
      </p>
      <code>
        <pre>
          {JSON.stringify(
            {
              Version: "2012-10-17",
              Statement: [
                {
                  Sid: "AllowS3F2Read",
                  Effect: "Allow",
                  Action: [
                    "s3:GetBucketLocation",
                    "s3:GetObject*",
                    "s3:ListBucket"
                  ],
                  Resource: [
                    `arn:aws:s3:::${bucket}`,
                    `arn:aws:s3:::${location.replace("s3://", "")}*`
                  ]
                },
                {
                  "Sid": "AllowS3F2Write",
                  "Effect": "Allow",
                  "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketRequestPayment",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:PutObject*"
                  ],
                  "Resource": [
                    `arn:aws:s3:::${bucket}`,
                    `arn:aws:s3:::${location.replace("s3://", "")}*`
                  ]
                },
                {
                  "Sid": "AllowS3F2Use",
                  "Effect": "Allow",
                  "Action": [
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*",
                    "kms:DescribeKey"
                  ],
                  "Resource": "<your-cmk-arn>"
                }, 
                {
                  "Sid": "AllowS3F2Grants",
                  "Effect": "Allow",
                  "Action": [
                    "kms:CreateGrant",
                    "kms:ListGrants",
                    "kms:RevokeGrant"
                  ],
                  "Resource": "<your-cmk-arn>",
                  "Condition": {
                    "Bool": {
                      "kms:GrantIsForAWSResource": "true"
                    }
                  }
                }
              ]
            },
            null,
            2
          )}
        </pre>
      </code>
    </li>
  </ol>  
</>)
