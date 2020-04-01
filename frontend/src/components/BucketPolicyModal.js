import React, { useState } from "react";
import { Button, Modal, Tabs, Tab } from "react-bootstrap";
import ReactJson from "react-json-view";
import Alert from "./Alert";

const { athenaExecutionRole, region } = window.s3f2Settings;

export default ({ accountId, bucket, roleArn, close, show, location }) => {
  const [key, setKey] = useState("bucket");
  const locationWithoutProtocol = location.replace("s3://", "");
  const tabs = [
    {
      key: "bucket",
      title: "Bucket Access",
      content: (
        <BucketPolicy
          bucket={bucket}
          accountId={accountId}
          location={locationWithoutProtocol}
          roleArn={roleArn}
        />
      )
    },
    {
      key: "kms",
      title: "KMS Access",
      content: (
        <KmsPolicy
          bucket={bucket}
          accountId={accountId}
          location={locationWithoutProtocol}
          roleArn={roleArn}
        />
      )
    }
  ];

  return (
    <Modal centered show={show} size="lg" onHide={close}>
      <Modal.Header closeButton>
        <Modal.Title>Generate Policies</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <p>
          After configuring a data mapper, you need to configure the relevant S3
          bucket, Customer Managed CMK and IAM policies to enable the solution
          to read and write from/to the bucket. The policy statements provided
          below are examples of how to grant the required access.
        </p>
        <Tabs activeKey={key} onSelect={k => setKey(k)}>
          {tabs.map(tab => (
            <Tab
              key={tab.key}
              eventKey={tab.key}
              title={<span>{tab.title}</span>}
            >
              {tab.content}
            </Tab>
          ))}
        </Tabs>
      </Modal.Body>
      <Modal.Footer>
        <Button className="aws-button action-button" onClick={close}>
          Close
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

const PolicyJson = ({ policy }) => (
  <ReactJson
    displayDataTypes={false}
    displayObjectSize={false}
    indentWidth={2}
    name={false}
    src={policy}
  />
);

const BucketPolicy = ({ bucket, accountId, location, roleArn }) => {
  const bucketPolicy = {
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "AllowS3F2Read",
        Effect: "Allow",
        Principal: {
          AWS: [
            `arn:aws:iam::${accountId}:role/${athenaExecutionRole}`,
            roleArn
          ]
        },
        Action: [
          "s3:GetBucket*",
          "s3:GetObject*",
          "s3:ListBucket*",
          "s3:ListMultipartUploadParts"
        ],
        Resource: [`arn:aws:s3:::${bucket}`, `arn:aws:s3:::${location}*`]
      },
      {
        Sid: "AllowS3F2Write",
        Effect: "Allow",
        Principal: { AWS: [roleArn] },
        Action: [
          "s3:AbortMultipartUpload",
          "s3:DeleteObjectVersion",
          "s3:PutObject*"
        ],
        Resource: [`arn:aws:s3:::${bucket}`, `arn:aws:s3:::${location}*`]
      }
    ]
  };
  return (
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
          Edit the Policy to grant the Athena Query Executor and Data Access IAM
          roles read/write access to the S3 Bucket and then choose{" "}
          <strong>Save</strong>. The following is an example bucket policy:
        </p>
        <PolicyJson policy={bucketPolicy} />
      </li>
    </ol>
  );
};

const KmsPolicy = ({ bucket, accountId, location, roleArn }) => {
  const keyPolicy = [
    {
      Sid: "AllowS3F2Usage",
      Effect: "Allow",
      Principal: {
        AWS: [`arn:aws:iam::${accountId}:role/${athenaExecutionRole}`, roleArn]
      },
      Action: [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      Resource: "*"
    },
    {
      Sid: "AllowS3F2Grants",
      Effect: "Allow",
      Principal: {
        AWS: [`arn:aws:iam::${accountId}:role/${athenaExecutionRole}`, roleArn]
      },
      Action: ["kms:CreateGrant", "kms:ListGrants", "kms:RevokeGrant"],
      Resource: "*",
      Condition: {
        Bool: {
          "kms:GrantIsForAWSResource": "true"
        }
      }
    }
  ];

  return (
    <>
      <Alert type="info" title="Additional Permissions">
        If your none of the objects stored in <strong>{location}</strong> are
        encrypted with a Customer Managed CMK, you can skip this step.
      </Alert>
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
            and then choose <strong>Key ID</strong> for the key used to encrypt
            objects in the {bucket} bucket.
          </p>
        </li>
        <li>
          <p>
            Update the Key Policy to add the Athena Query Executor and Data
            Access IAM roles as <strong>Key users</strong>. If using the policy
            view, here are example statements you can add to the key policy to
            grant the required access:
          </p>
          <PolicyJson policy={keyPolicy} />
        </li>
      </ol>
    </>
  );
};
