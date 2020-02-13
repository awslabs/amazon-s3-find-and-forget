import React from "react";

import Icon from "../Icon";
import { docsUrl, repoUrl } from "../../utils";

const links = [
  {
    title: "Deletion Job Architecture",
    url: repoUrl("ARCHITECTURE.md#deletion-job-workflow")
  },
  {
    title: "Start a Deletion Job",
    url: docsUrl("USER_GUIDE.md#running-a-deletion-job")
  },
  {
    title: "Troubleshooting",
    url: docsUrl("TROUBLESHOOTING.md")
  }
];

export default () => (
  <>
    <h2>Deletion Jobs</h2>
    <p className="separator-top">
      A <strong>Deletion Job</strong> is an activity performed by Amazon S3 Find
      and Forget which queries your data in S3 defined by the Data Mappers and
      deletes rows containing any <strong>Match</strong> present in the Deletion
      Queue. To view the details of a job, choose the <strong>Job ID</strong>{" "}
      from the Deletion Job list.
    </p>
    <p className="separator-bottom">
      Only one deletion job can be running at any given time. Before you retry a
      failed job, be sure to troubleshoot and fix problems first.
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
