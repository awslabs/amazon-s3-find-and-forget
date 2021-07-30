export const formatErrorMessage = e => {
  if (e && e.response) {
    if (e.response.data && e.response.data.Message) {
      return e.response.data.Message;
    } else {
      return `Unknown error (${e.response.status} status code)`;
    }
  }

  return e ? e.toString() : "Unknown error";
};

export const isArray = x => Array.isArray(x);

export const isEmpty = x =>
  x === null ||
  isUndefined(x) ||
  (isArray(x)
    ? x.length === 0
    : typeof x === "string"
    ? x.trim() === ""
    : false);

export const isIdValid = x => {
  const idRegex = /^[a-zA-Z0-9]+$/;
  return idRegex.test(x);
};

export const isRoleArnValid = x => {
  const arnRegex = /^arn:(aws[a-zA-Z-]*)?:iam::\d{12}:role\/S3F2DataAccessRole$/;
  return arnRegex.test(x);
};

export const sortBy = (obj, key) =>
  obj.sort((a, b) => (a[key] > b[key] ? 1 : a[key] < b[key] ? -1 : 0));

export const daysSinceDateTime = x => {
  const now = new Date();
  const from = x ? new Date(x * 1000) : now;
  const aDay = 24 * 60 * 60 * 1000;
  return parseInt((now - from) / aDay, 10);
};

export const formatDateTime = x => (x ? new Date(x * 1000).toUTCString() : "-");

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
    case "FORGET_COMPLETED_CLEANUP_IN_PROGRESS":
      return "info";
    default:
      return "error";
  }
};

export const last = x => (x.length > 0 ? x[x.length - 1] : null);

const trimTrailingSlash = x => x.replace(/\/+$/, "");
const trimLeadingSlash = x => x.replace(/\/+$/, "");

export const repoUrl = x => {
  const baseUrl = trimTrailingSlash(
    process.env.REACT_APP_REPO_URL.replace("git+", "").replace(".git", "")
  );
  const path = trimLeadingSlash(x);
  return trimTrailingSlash(`${baseUrl}/${path}`);
};

export const docsUrl = x => repoUrl(`blob/master/docs/${trimLeadingSlash(x)}`);

export const findMin = (arr, key) =>
  arr.reduce((prev, curr) => (prev[key] < curr[key] ? prev : curr));

export const multiValueArrayReducer = (state, action) => {
  if (action.type === "add" && !state.includes(action.value))
    return [...state, action.value];
  if (action.type === "remove" && state.includes(action.value))
    return state.filter(x => x !== action.value);
  if (action.type === "reset") return action.value || [];
  return state;
};
