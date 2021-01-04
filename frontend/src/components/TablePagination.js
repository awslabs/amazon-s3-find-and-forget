import React, { useState } from "react";
import { Button } from "react-bootstrap";

import Icon from "./Icon";

import { getPagesList } from "../utils/paginator";

const TablePagination = ({ onPageChange, pages }) => {
  const [currentPage, setCurrentPage] = useState(0);

  const setPage = index => {
    setCurrentPage(index);
    onPageChange(index);
  };

  const pagesList = getPagesList(pages, currentPage);

  return (
    <>
      <Button
        disabled={currentPage === 0}
        onClick={() => setPage(currentPage - 1)}
        variant="link"
      >
        <Icon type="arrow-prev" />
      </Button>
      {pagesList.map((page, i) =>
        page === "..." ? (
          <Button disabled={true} key={i} variant="link">
            ...
          </Button>
        ) : (
          <Button
            className={currentPage === page ? "selected" : ""}
            key={i}
            onClick={() => setPage(page)}
            variant="link"
          >
            {page + 1}
          </Button>
        )
      )}
      <Button
        disabled={currentPage === pages - 1}
        onClick={() => setPage(currentPage + 1)}
        variant="link"
      >
        <Icon type="arrow-next" />
      </Button>
    </>
  );
};

export default TablePagination;
