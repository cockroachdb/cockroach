import { Location } from "history";
import { match as Match } from "react-router-dom";

interface ParamsObj {
  [key: string]: string;
}

/* eslint-disable no-prototype-builtins,no-restricted-syntax */
export function queryToString(obj: any) {
  const str = [];
  for (const p in obj) {
    if (obj.hasOwnProperty(p)) {
      str.push(`${encodeURIComponent(p)}=${encodeURIComponent(obj[p])}`);
    }
  }
  return str.join("&");
}

export function queryToObj(location: Location, key: string, value: string) {
  const params = new URLSearchParams(location.search);
  const paramObj: ParamsObj = {};

  params.forEach((v, k) => {
    paramObj[k] = v;
  });

  if (key && value) {
    if (value.length > 0 || typeof value === "number") {
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

export function getMatchParamByName(match: Match<any>, key: string) {
  const param = match.params[key];
  if (param) {
    return decodeURIComponent(param);
  }
  return null;
}
