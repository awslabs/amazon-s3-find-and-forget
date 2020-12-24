import React from "react";

import Icon from "../Icon";
import { docsUrl } from "../../utils";

const links = [
  {
    title: "Deletion Job Status Reference",
    url: docsUrl("USER_GUIDE.md#deletion-job-statuses"),
  },
  {
    title: "Understanding Deletion Job Events",
    url: docsUrl("USER_GUIDE.md#deletion-job-event-types"),
  },
  {
    title: "Troubleshooting",
    url: docsUrl("TROUBLESHOOTING.md"),
  },
];

const DeletionJobDetails = () => (
  <>
    <h2>Deletion Job Details</h2>
    <p className="separator-top">
      You can view the current status and associated stats for the Deletion Job.
      Consult the{" "}
      <a
        href={docsUrl("USER_GUIDE.md#deletion-job-statuses")}
        target="_blank"
        rel="noopener noreferrer"
        className="learnMoreLink"
      >
        job status
      </a>{" "}
      documentation for more information on possible job statuses and their
      meaning. You can use the Job Events list to see all the events that have
      occurred for the current job. To view the raw event data, choose{" "}
      <strong>View Event</strong> for a row in the Job Event list.
    </p>
    <p className="separator-bottom">
      If a job finishes with a status other than <strong>COMPLETED</strong>,
      troubleshoot and fix any problems before retrying the job.
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

export default DeletionJobDetails;
