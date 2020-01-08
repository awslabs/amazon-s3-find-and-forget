import React, { useState } from "react";
import { Button } from "react-bootstrap";

import Icon from "./Icon";
import "./AppLayout.css";

import { isUndefined } from "../utils";

export default ({ currentPage, onMenuClick, pages }) => {
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
          <h2>Amazon S3 Find &amp; Forget</h2>
          <p className="separator-top separator-bottom">
            {pages
              .filter(p => !p.parent)
              .map((page, index) => {
                const classes = ["menu"];
                const selected =
                  index === currentPage || index === pages[currentPage].parent;
                if (selected) classes.push("selected");
                return (
                  <Button
                    className={classes.join(" ")}
                    variant="link"
                    key={index}
                    onClick={() => onMenuClick(index)}
                  >
                    {page.title}
                  </Button>
                );
              })}
          </p>
        </div>
      </div>
      <div id="main">
        <div className="breadcrumbs">
          <Button variant="link" onClick={() => onMenuClick(0)}>
            S3 Find &amp; Forget
          </Button>
          <Icon type="breadcrumb" />
          {!isUndefined(pages[currentPage].parent) && (
            <>
              <Button
                variant="link"
                onClick={() => onMenuClick(pages[currentPage].parent)}
              >
                {pages[pages[currentPage].parent].title}
              </Button>
              <Icon type="breadcrumb" />
            </>
          )}
          <span className="breadcrumb-last">{pages[currentPage].title}</span>
        </div>
        <div className="content">{pages[currentPage].page}</div>
      </div>
      {pages[currentPage].help && (
        <div id="right" className={right}>
          <div className="icon" onClick={() => setRightOpen(!rightOpen)}>
            <Icon type={rightOpen ? "close" : "info"} />
          </div>
          <div className="content">{pages[currentPage].help}</div>
        </div>
      )}
    </div>
  );
};
