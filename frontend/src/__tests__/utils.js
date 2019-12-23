import {
  arrayItemsAnyEmpty,
  isEmpty,
  isIdValid,
  retryWrapper,
  sortBy
} from "../utils";

test("retryWrapper: retries no times if no failure", async () => {
  let attempts = 0;
  const consoleLogSpy = jest.spyOn(console, "log").mockImplementation();

  const success = () =>
    new Promise(resolve => {
      attempts++;
      return resolve("success");
    });

  const wrapped = () => retryWrapper(success);
  const result = await wrapped();

  expect(result).toEqual("success");
  expect(attempts).toEqual(1);
  expect(consoleLogSpy.mock.calls.length).toEqual(0);

  consoleLogSpy.mockRestore();
});

test("retryWrapper: retries one time if one failure", async () => {
  let attempts = 0;
  const timerOverride = 1; // override timer to 1ms to speed up tests
  const consoleLogSpy = jest.spyOn(console, "log").mockImplementation();

  const f = () =>
    new Promise((resolve, reject) => {
      attempts++;
      if (attempts === 1) return reject("fail");
      return resolve("success");
    });

  const wrapped = () => retryWrapper(f, timerOverride);
  const result = await wrapped();

  expect(result).toEqual("success");
  expect(attempts).toEqual(2);
  expect(consoleLogSpy.mock.calls).toEqual([["Retry n. 1 in 0.002s..."]]);

  consoleLogSpy.mockRestore();
});

test("retryWrapper: retries 3 times with exponential back-off", () => {
  let attempts = 0;
  const timerOverride = 1; // override timer to 1ms to speed up tests
  const consoleLogSpy = jest.spyOn(console, "log").mockImplementation();

  const failure = () =>
    new Promise((resolve, reject) => {
      attempts++;
      return reject("fail");
    });

  const wrapped = () => retryWrapper(failure, timerOverride);

  return wrapped().catch(e => {
    expect(e).toEqual("fail");
    expect(attempts).toEqual(4);

    expect(consoleLogSpy.mock.calls).toEqual([
      ["Retry n. 1 in 0.002s..."],
      ["Retry n. 2 in 0.004s..."],
      ["Retry n. 3 in 0.008s..."]
    ]);

    consoleLogSpy.mockRestore();
  });
});

test("isEmpty", () => {
  const scenarios = [
    { test: [], expected: true },
    { test: [1], expected: false },
    { test: null, expected: true },
    { test: undefined, expected: true },
    { test: "", expected: true },
    { test: "foo", expected: false }
  ];

  scenarios.forEach(scenario =>
    expect(isEmpty(scenario.test)).toEqual(scenario.expected)
  );
});

test("isIdValid", () => {
  const scenarios = [
    { test: "abc_ABC-123", expected: true },
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

test("arrayItemsAnyEmpty", () => {
  const scenarios = [
    { test: [], expected: false },
    { test: ["123", ""], expected: true },
    { test: ["123", undefined, 345], expected: true },
    { test: ["123", "abc", null], expected: true },
    { test: ["123"], expected: false },
    { test: ["123", "abc"], expected: false }
  ];

  scenarios.forEach(scenario =>
    expect(arrayItemsAnyEmpty(scenario.test)).toEqual(scenario.expected)
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
