import React from "react";

import Icon from "../Icon";
import { docsUrl } from "../../utils";

const links = [
  {
    title: "Add a Data Mapper",
    url: docsUrl("USER_GUIDE.md#configuring-data-mappers"),
  },
  {
    title: "Troubleshooting",
    url: docsUrl("TROUBLESHOOTING.md"),
  },
];

const NewDataMapper = () => (
  <>
    <h2>Create a Data Mapper</h2>
    <p className="separator-top">
      Data mappers enable you to connect data hosted in S3 to the Amazon S3 Find
      and Forget solution.
    </p>
    <p>
      When creating a data mapper you need to choose a table in a supported{" "}
      <strong>data catalog provider</strong> which describes the location and
      structure of the data you want to connect to the solution. Currently,{" "}
      <strong>AWS Glue</strong> is the only supported data catalog provider. You
      must also choose a <strong>query executor</strong> which is the service
      the Amazon S3 Find and Forget solution will use to query the data.
      Currently, <strong>Amazon Athena</strong> is the only supported query
      executor. After selecting a query executor, you must to choose the columns
      in which to search for matches. The columns for your data are defined in
      the data catalog table.
    </p>
    <p>
      After creating a data mapper, you must update the S3 Bucket Policy for the
      S3 Bucket referenced by the data mapper to grant read permission to the
      IAM role assumed by the query executor (e.g. Amazon Athena), and
      read/write permission to the IAM role used by AWS Fargate to perform any
      required deletions.
    </p>
    <p className="separator-bottom">
      The deletion task requires you to have provisioned the{" "}
      <strong>S3F2DataAccessRole</strong> in the account which owns the bucket
      referenced by the data mapper. For more information on how to provision
      this role, check the{" "}
      <a
        href={docsUrl("USER_GUIDE.md#provisioning-data-access-iam-roles")}
        target="_blank"
        rel="noopener noreferrer"
        className="learnMoreLink"
      >
        user guide
      </a>
      .
    </p>
    <h3>
      Learn more <Icon type="new-window" />
    </h3>
    <ul>
      {links.map((link, index) => (
        <li key={index}>
          <a
            href={link.url}
            target="_blank"
            rel="noopener noreferrer"
            className="learnMoreLink"
          >
            {link.title}
          </a>
        </li>
      ))}
    </ul>
  </>
);

export default NewDataMapper;
