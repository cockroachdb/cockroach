import { Location } from "history";


/* eslint-disable no-prototype-builtins,no-restricted-syntax */
export function queryToString(obj) {
  const str = [];
  for (const p in obj) {
    if (obj.hasOwnProperty(p)) {
      str.push(`${encodeURIComponent(p)}=${encodeURIComponent(obj[p])}`);
    }
  }
  return str.join('&');
}

export function queryToObj(location: Location, key: string, value: string) {
  const params = new URLSearchParams(location.search);
  const paramObj = {};
  for (const data of params.keys()) {
    paramObj[data] = params.get(data);
  }
  if (key) {
    if (value.length > 0 || (typeof value === "number")) {
      paramObj[key] = value;
    } else {
      delete paramObj[key];
    }
  }
  return paramObj;
}

export function queryByName(location: Location, key: string) {
  const urlParams = new URLSearchParams(location.search);
  return urlParams.get(key);
}
