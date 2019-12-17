import React from "react";

import Icon from "../Icon";

const links = [
  {
    title: "Getting Started",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Setup Data Mappers",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Start a Deletion Job",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Troubleshooting",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  },
  {
    title: "Github Repository",
    url: "https://github.com/awslabs/amazon-s3-find-and-forget"
  }
];

export default () => (
  <>
    <h2>Dashboard</h2>

    <p className="separator-top separator-bottom">
      Here you can view some metrics about your solution and start a new
      deletion job. Use the menu on the left to access the appropriate section.
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
