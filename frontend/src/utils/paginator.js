import { last } from "./index";

const EXTRA_PAGES = 1;

export const getPagesList = (pages, currentPage) => {
  const pagesList = [];
  if (pages > 0) {
    pagesList.push(0);

    if (currentPage - EXTRA_PAGES === 2) pagesList.push(1);
    else if (currentPage - EXTRA_PAGES > 2) pagesList.push("...");

    for (
      let i = currentPage - EXTRA_PAGES;
      i < currentPage + EXTRA_PAGES + 1;
      i++
    ) {
      if (i > 0 && i < pages && !pagesList.includes(i)) pagesList.push(i);
    }

    if (last(pagesList) === pages - 3) pagesList.push(pages - 2);
    else if (last(pagesList) < pages - 2) pagesList.push("...");

    if (last(pagesList) !== pages - 1) pagesList.push(pages - 1);
  }

  return pagesList;
};
