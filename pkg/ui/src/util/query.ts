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
