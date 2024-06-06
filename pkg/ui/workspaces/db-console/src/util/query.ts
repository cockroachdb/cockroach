// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Location } from "history";
import isNull from "lodash/isNull";
import isUndefined from "lodash/isUndefined";
import compact from "lodash/compact";
import map from "lodash/map";
import { match as Match } from "react-router-dom";

interface ParamsObj {
  [key: string]: string;
}

interface URLSearchParamsWithKeys extends URLSearchParams {
  keys: () => string;
}

// propsToQueryString is a helper function that converts a set of object
// properties to a query string
// - keys with null or undefined values will be skipped
// - non-string values will be toString'd
export function propsToQueryString(props: { [k: string]: any }): string {
  return compact(
    map(props, (v: any, k: string) =>
      !isNull(v) && !isUndefined(v)
        ? `${encodeURIComponent(k)}=${encodeURIComponent(v.toString())}`
        : null,
    ),
  ).join("&");
}

export function queryToObj(location: Location, key: string, value: string) {
  const params = new URLSearchParams(
    location.search,
  ) as URLSearchParamsWithKeys;
  const paramObj: ParamsObj = {};
  for (const data of params.keys()) {
    paramObj[data] = params.get(data);
  }
  if (key && value) {
    if (value.length > 0 || typeof value === "number") {
      paramObj[key] = value;
    } else {
      delete paramObj[key];
    }
  }
  return paramObj;
}

export function queryByName(location: Location, key: string): string | null {
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
