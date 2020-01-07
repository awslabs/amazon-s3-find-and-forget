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
  x === null ||
  isUndefined(x) ||
  (Array.isArray(x)
    ? x.length === 0
    : typeof x === "string"
    ? x.trim() === ""
    : false);

export const isIdValid = x => {
  const idRegex = /^[a-zA-Z0-9]+$/;
  return idRegex.test(x);
};

export const sortBy = (obj, key) =>
  obj.sort((a, b) => (a[key] > b[key] ? 1 : a[key] < b[key] ? -1 : 0));

export const daysSinceDateTime = x => {
  const now = new Date();
  const from = x ? new Date(x * 1000) : now;
  const aDay = 24 * 60 * 60 * 1000;
  return parseInt((now - from) / aDay, 10);
};

export const formatDateTime = x => {
  return x ? new Date(x * 1000).toUTCString() : "-"
};

export const formatFileSize = x => {
  const units = ["bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

  if (x === 1) return "1 byte";

  let l = 0;
  let n = parseInt(x, 10) || 0;

  while (n >= 1024 && ++l) n = n / 1024;

  return n.toFixed(n < 10 && l > 0 ? 1 : 0) + " " + units[l];
};

export const isUndefined = x => typeof x === "undefined";

export const withDefault = (x, formatter = () => x) =>
  isEmpty(x) ? "-" : formatter(x);

export const successJobClass = status => {
  switch (status) {
    case "COMPLETED":
      return "success";
    case "RUNNING":
    case "QUEUED":
      return "info";
    default:
      return "error";
  }
};
