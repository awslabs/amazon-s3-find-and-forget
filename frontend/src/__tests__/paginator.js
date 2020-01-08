import { getPagesList } from "../utils/paginator";

test("getPagesList", () => {
  const scenarios = [
    { test: { pages: 7, currentPage: 0 }, expected: [0, 1, "...", 6] },
    { test: { pages: 7, currentPage: 5 }, expected: [0, "...", 4, 5, 6] },
    { test: { pages: 7, currentPage: 6 }, expected: [0, "...", 5, 6] },
    { test: { pages: 7, currentPage: 3 }, expected: [0, 1, 2, 3, 4, 5, 6] },
    { test: { pages: 7, currentPage: 4 }, expected: [0, "...", 3, 4, 5, 6] },
    {
      test: { pages: 10, currentPage: 5 },
      expected: [0, "...", 4, 5, 6, "...", 9]
    },
    { test: { pages: 4, currentPage: 2 }, expected: [0, 1, 2, 3] },
    {
      test: { pages: 20, currentPage: 13 },
      expected: [0, "...", 12, 13, 14, "...", 19]
    }
  ];

  scenarios.forEach(scenario =>
    expect(
      getPagesList(scenario.test.pages, scenario.test.currentPage)
    ).toEqual(scenario.expected)
  );
});
