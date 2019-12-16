import { retryWrapper } from "../utils";

test("retryWrapper retries no times if no failure", async () => {
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

test("retryWrapper retries one time if one failure", async () => {
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

test("retryWrapper retries 3 times with exponential back-off", () => {
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
