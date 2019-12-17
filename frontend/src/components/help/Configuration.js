import React from "react";

import Icon from "../Icon";

const links = [
  {
    title: "Setup Data Mappers",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Setup and Test IAM Configuration",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Troubleshooting",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  }
];

export default () => (
  <>
    <h2>Configuration</h2>
    <p className="separator-top">
      Here you can setup the Data Mappers associated to your account. Data
      Mappers are the link between your data catalog and the Amazon S3 Find and
      Forget solution.
    </p>
    <p className="separator-bottom">
      After configuring the data mappers, you need to configure the relevant S3
      bucket policies to enable the solution to write and read from/to them.
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
