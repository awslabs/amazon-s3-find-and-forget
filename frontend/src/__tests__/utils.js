import {
  formatErrorMessage,
  formatDateTime,
  formatFileSize,
  isEmpty,
  isIdValid,
  isRoleArnValid,
  isUndefined,
  last,
  sortBy,
  withDefault,
  repoUrl,
  docsUrl,
} from "../utils";

test("isEmpty", () => {
  const scenarios = [
    { test: [], expected: true },
    { test: [1], expected: false },
    { test: null, expected: true },
    { test: undefined, expected: true },
    { test: "", expected: true },
    { test: "foo", expected: false },
    { test: 123, expected: false },
    { test: 0, expected: false }
  ];

  scenarios.forEach(scenario =>
    expect(isEmpty(scenario.test)).toEqual(scenario.expected)
  );
});

test("isIdValid", () => {
  const scenarios = [
    { test: "abc_ABC-123", expected: false },
    { test: "abcABC123", expected: true },
    { test: "def", expected: true },
    { test: "GHI", expected: true },
    { test: "67890", expected: true },
    { test: "ab cd", expected: false },
    { test: "ab@cd", expected: false },
    { test: "ab\\c23", expected: false },
    { test: "ab.33", expected: false }
  ];

  scenarios.forEach(scenario =>
    expect(isIdValid(scenario.test)).toEqual(scenario.expected)
  );
});

test("isRoleArnValid", () => {
  const scenarios = [
    { test: "abc_ABC-123", expected: false },
    { test: "arn:aws:iam::123456789012:role/OtherRole", expected: false },
    { test: "arn:aws:iam::123456789012:role/S3F2DataAccessRole", expected: true },
  ];

  scenarios.forEach(scenario =>
    expect(isRoleArnValid(scenario.test)).toEqual(scenario.expected)
  );
});

test("sortBy", () => {
  const scenarios = [
    { test: [{ a: "ee" }, { a: "cc" }], expected: [{ a: "cc" }, { a: "ee" }] },
    { test: [{ a: 45 }, { a: 13 }], expected: [{ a: 13 }, { a: 45 }] }
  ];

  scenarios.forEach(scenario =>
    expect(sortBy(scenario.test, "a")).toEqual(scenario.expected)
  );
});

test("formatDateTime", () => {
  expect(formatDateTime(1578405187)).toEqual("Tue, 07 Jan 2020 13:53:07 GMT");
});

test("formatDateTime undefined arg", () => {
  expect(formatDateTime()).toEqual("-");
});

test("formatFileSize", () => {
  const scenarios = [
    { test: 1, expected: "1 byte" },
    { test: 400, expected: "400 bytes" },
    { test: 3398, expected: "3.3 KB" },
    { test: 459234, expected: "448 KB" },
    { test: 6234567, expected: "5.9 MB" },
    { test: 35895345, expected: "34 MB" },
    { test: 403212340, expected: "385 MB" },
    { test: 60245566785, expected: "56 GB" },
    { test: 2345673567346763, expected: "2.1 PB" }
  ];

  scenarios.forEach(scenario =>
    expect(formatFileSize(scenario.test)).toEqual(scenario.expected)
  );
});

test("isUndefined", () => {
  expect(isUndefined(undefined)).toEqual(true);
  expect(isUndefined(0)).toEqual(false);
  expect(isUndefined(null)).toEqual(false);
  expect(isUndefined("")).toEqual(false);
});

test("withDefault", () => {
  expect(withDefault(0)).toEqual(0);
  expect(withDefault(undefined)).toEqual("-");
  expect(withDefault("")).toEqual("-");
});

test("last", () => {
  expect(last([1, 2, 3])).toEqual(3);
  expect(last([1])).toEqual(1);
  expect(last([])).toEqual(null);
});

test("formatErrorMessage", () => {
  expect(formatErrorMessage()).toEqual("Unknown error");
  expect(formatErrorMessage("Error message!")).toEqual("Error message!");
  expect(
    formatErrorMessage({
      response: {
        status: 400
      }
    })
  ).toEqual("Unknown error (400 status code)");
  expect(
    formatErrorMessage({
      response: {
        status: 422,
        data: { Message: "Error from API" }
      }
    })
  ).toEqual("Error from API");
});


describe("urls", () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    jest.resetModules()
    process.env = { ...OLD_ENV, REACT_APP_REPO_URL: "git+https://example.com" };
  });

  afterEach(() => {
    process.env = OLD_ENV;
  });

  test("repoUrl", () => {
      expect(repoUrl("/")).toEqual("https://example.com");
      expect(repoUrl("test")).toEqual("https://example.com/test");
  })

  test("docsUrl", () => {
      expect(docsUrl("/")).toEqual("https://example.com/blob/master/docs");
      expect(docsUrl("test")).toEqual("https://example.com/blob/master/docs/test");
  })
});
