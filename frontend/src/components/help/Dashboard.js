import React from "react";

import Icon from "../Icon";
import {repoUrl, docsUrl} from "../../utils";

const links = [
  {
    title: "Getting Started",
    url: repoUrl("/")
  },
  {
    title: "Solution Architecture",
    url: docsUrl("ARCHITECTURE.md")
  },
  {
    title: "Add a Data Mapper",
    url: docsUrl("USER_GUIDE.md#configuring-data-mappers")
  },
  {
    title: "Add a Match to the Deletion Queue",
    url: docsUrl("USER_GUIDE.md#adding-to-the-deletion-queue")
  },
  {
    title: "Start a Deletion Job",
    url: docsUrl("USER_GUIDE.md#running-a-deletion-job")
  },
  {
    title: "Github Repository",
    url: repoUrl("/")
  }
];

export default () => (
  <>
    <h2>Dashboard</h2>
    <p className="separator-top separator-bottom">
      You can view key metrics about this deployment of the Amazon S3 Find and
      Forget solution. Choose <strong>Start a Deletion Job</strong> to execute
      a deletion job for the matches currently in the deletion queue.
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
