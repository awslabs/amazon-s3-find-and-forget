import React, { useState } from "react";
import { Button } from "react-bootstrap";

import Icon from "./Icon";
import "./AppLayout.css";

export default ({ children }) => {
  const [leftOpen, setLeftOpen] = useState(true);
  const [rightOpen, setRightOpen] = useState(false);

  const left = leftOpen ? "open" : "closed";
  const right = rightOpen ? "open" : "closed";

  return (
    <div id="layout">
      <div id="left" className={left}>
        <div className="icon" onClick={() => setLeftOpen(!leftOpen)}>
          <Icon type={leftOpen ? "close" : "hamburger"} />
        </div>
        <div className="content">
          <h2>Amazon S3 Find and Replace</h2>
          <p className="separator-top separator-bottom">
            <Button className="menu selected" variant="link">
              Dashboard
            </Button>
            <Button className="menu" variant="link">
              Configuration
            </Button>
            <Button className="menu" variant="link">
              Deletion Queue
            </Button>
            <Button className="menu" variant="link">
              Reports
            </Button>
          </p>
        </div>
      </div>
      <div id="main">
        <div className="content">{children}</div>
      </div>

      <div id="right" className={right}>
        <div className="icon" onClick={() => setRightOpen(!rightOpen)}>
          <Icon type={rightOpen ? "close" : "info"} />
        </div>
        <div className="content">
          <h2>Dashboard</h2>

          <p className="separator-top separator-bottom">
            Here you can view the Data Mappers associated to your account, the
            Deletion Queue and Reports. Use the menu on the left to access the
            appropriate section.
          </p>
          <h3>
            Learn more <Icon type="new-window" />
          </h3>
          <ul>
            <li>
              <a
                href="https://github.com/awslabs/amazon-s3-find-and-replace"
                target="_blank"
                rel="noopener noreferrer"
                className="learnMoreLink"
              >
                Setup Data Mappers
              </a>
            </li>
            <li>
              <a
                href="https://github.com/awslabs/amazon-s3-find-and-replace"
                target="_blank"
                rel="noopener noreferrer"
                className="learnMoreLink"
              >
                Test IAM permissions
              </a>
            </li>
            <li>
              <a
                href="https://github.com/awslabs/amazon-s3-find-and-replace"
                target="_blank"
                rel="noopener noreferrer"
                className="learnMoreLink"
              >
                Start a Deletion Job
              </a>
            </li>
            <li>
              <a
                href="https://github.com/awslabs/amazon-s3-find-and-replace"
                target="_blank"
                rel="noopener noreferrer"
                className="learnMoreLink"
              >
                Troubleshooting
              </a>
            </li>
            <li>
              <a
                href="https://github.com/awslabs/amazon-s3-find-and-replace"
                target="_blank"
                rel="noopener noreferrer"
                className="learnMoreLink"
              >
                Github Repository
              </a>
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
};
