// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Location, History } from "history";
import { match as Match } from "react-router-dom";

interface ParamsObj {
  [key: string]: string;
}

// propsToQueryString is a helper function that converts a set of object
// properties to a query string
// - keys with null or undefined values will be skipped
// - non-string values will be toString'd
export function propsToQueryString(props: { [k: string]: unknown }): string {
  const params = new URLSearchParams();
  Object.entries(props).forEach(
    ([k, v]: [string, unknown]) => v != null && params.set(k, v.toString()),
  );
  return params.toString();
}

export function queryToObj(
  location: Location,
  key: string,
  value: string,
): ParamsObj {
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

export function queryByName(location: Location, key: string): string {
  const urlParams = new URLSearchParams(location.search);
  return urlParams.get(key);
}

export function getMatchParamByName(
  match: Match<Record<string, string>>,
  key: string,
): string | null {
  const param = match.params[key];
  if (param) {
    return decodeURIComponent(param);
  }
  return null;
}

export function syncHistory(
  params: Record<string, string | undefined>,
  history: History,
  push?: boolean,
): void {
  const nextSearchParams = new URLSearchParams(history.location.search);

  Object.entries(params).forEach(([key, value]) => {
    if (!value) {
      nextSearchParams.delete(key);
    } else {
      nextSearchParams.set(key, value);
    }
  });

  history.location.search = nextSearchParams.toString();
  if (push) {
    history.push(history.location);
  } else {
    history.replace(history.location);
  }
}
