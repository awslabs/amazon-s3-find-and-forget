import React from "react";

import Icon from "../Icon";
import {docsUrl} from "../../utils";

const links = [
  {
    title: "Working with the Deletion Queue",
    url: docsUrl("USER_GUIDE.md#matches")
  },
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
    <h2>Deletion Queue</h2>
    <p className="separator-top">
      You can view the current <strong>matches</strong> in the deletion queue.
      A match is a value you wish to search for which identifies rows in your S3
      data lake to be deleted. For example, a match could be the ID
      of a specific customer.
    </p>
    <p className="separator-bottom">
      To add a new match to the deletion queue, choose <strong>Add Match
      to the Deletion Queue</strong>. To remove a Match from the Deletion
      Queue, choose the match from the list and then choose <strong>Remove
      </strong>. You cannot remove items from the Deletion Queue whilst
      there is a job in progress.
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
