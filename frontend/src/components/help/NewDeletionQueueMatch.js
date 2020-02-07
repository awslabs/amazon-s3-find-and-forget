import React from "react";

import Icon from "../Icon";
import {docsUrl} from "../../utils";

const links = [
  {
    title: "Add a Match to the Deletion Queue",
    url: docsUrl("USER_GUIDE.md#adding-to-the-deletion-queue")
  },
  {
    title: "Troubleshooting",
    url: docsUrl("TROUBLESHOOTING.md")
  }
];

export default () => (
  <>
    <h2>Add to the Deletion Queue</h2>
    <p className="separator-top">
      Items in the Deletion Queue are known as <strong>matches</strong>. A
      match is used by Amazon S3 Find and Forget to identify rows in your
      data to delete. Once a job completes, the matches processed by the job
      will be removed from the queue automatically. If a job does not complete
      successfully, the matches will remain in the Deletion Queue.
    </p>
    <p className="separator-bottom">
      When adding a match to the Deletion Queue you must first enter the match
      value then choose the Data Mappers to use to search for the value. By
      default, Amazon S3 Find and Forget will search all data mappers. You
      cannot change the selected data mappers for a match already in the
      Deletion Queue. You must first delete the match from the queue then
      re-add it.
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
