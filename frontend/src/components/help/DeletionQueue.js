import React from "react";

import Icon from "../Icon";

const links = [
  {
    title: "Deletion Job Architecture",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Start a Deletion Job",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Troubleshooting",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  }
];

export default () => (
  <>
    <h2>Deletion Queue</h2>

    <p className="separator-top separator-bottom">
      Here you can add users to the deletion queue. When adding a user in the
      queue, it is possible to specify where to delete it: all data mappers
      (default) or specific data mappers.
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
