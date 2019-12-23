const MAX_RETRIES = 3;
const RETRY_START = 1000;

export const retryWrapper = (p, timeout, retryN) =>
  new Promise((resolve, reject) =>
    p()
      .then(resolve)
      .catch(e => {
        if (retryN === MAX_RETRIES) return reject(e);
        const t = (timeout || RETRY_START / 2) * 2;
        const r = (retryN || 0) + 1;
        console.log(`Retry n. ${r} in ${t / 1000}s...`);
        setTimeout(
          () =>
            retryWrapper(p, t, r)
              .then(resolve)
              .catch(reject),
          t
        );
      })
  );

export const formatErrorMessage = e => {
  let msg = e.toString() || "An error happened";
  if (e.response) {
    if (e.response.status) msg += ` (${e.response.status} status code)`;
    if (e.response.data && e.response.data.Message)
      msg += `: ${e.response.data.Message}`;
  }

  return msg;
};

export const isEmpty = x =>
  !x || (Array.isArray(x) ? x.length === 0 : x.trim() === "");

export const isIdValid = x => {
  const idRegex = /^[a-zA-Z0-9-_]+$/;
  return idRegex.test(x);
};

export const arrayItemsAnyEmpty = x => {
  const arrayItemsAnyEmptyReducer = (a, v) => a || isEmpty(v);
  return !x || x.reduce(arrayItemsAnyEmptyReducer, false);
};

export const sortBy = (obj, key) =>
  obj.sort((a, b) => (a[key] > b[key] ? 1 : a[key] < b[key] ? -1 : 0));

export const daysSinceDateTime = x => {
  const now = new Date();
  const from = new Date(x);
  const aDay = 24 * 60 * 60 * 1000;
  return parseInt((now - from) / aDay, 10);
};
