import React from "react";

import Icon from "../Icon";
import { docsUrl } from "../../utils";

const links = [
  {
    title: "Working with Data Mappers",
    url: docsUrl("USER_GUIDE.md#data-mappers")
  },
  {
    title: "Add a Data Mapper",
    url: docsUrl("USER_GUIDE.md#configuring-data-mappers")
  },
  {
    title: "Setup and Test Data Access",
    url: docsUrl("USER_GUIDE.md#granting-access-to-data")
  },
  {
    title: "Troubleshooting",
    url: docsUrl("TROUBLESHOOTING.md")
  }
];

export default () => (
  <>
    <h2>Data Mappers</h2>
    <p className="separator-top">
      You can view the Data Mappers currently configured in this Amazon S3 Find
      and Forget deployment. Data Mappers instruct the Amazon S3 Find and Forget
      solution how and where to search for items to be deleted. To configure a
      new Data Mapper, choose <strong>Create Data Mapper</strong>.
    </p>
    <p className="separator-bottom">
      After configuring a Data Mapper, you need to configure the relevant S3
      bucket policies to enable the solution to read and write the data in the
      location specified by the Data Mapper. To see the required policies,
      choose a Data Mapper from the list then choose{" "}
      <strong>View S3 Bucket Policy</strong>.
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
