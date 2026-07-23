// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import moment from "moment-timezone";

import { VersionList, VersionCheckRequest } from "src/interfaces/cockroachlabs";

import { withTimeout } from "./api";

export const COCKROACHLABS_ADDR = "https://register.cockroachdb.com";

interface FetchConfig {
  method?: string;
  timeout?: moment.Duration;
}

// TODO(maxlang): might be possible to consolidate with Fetch in api.ts
function timeoutFetch<T extends BodyInit, R>(
  url: string,
  req?: T,
  config: FetchConfig = {},
): Promise<R> {
  return withTimeout(
    fetch(url, {
      method: config.method || (req ? "POST" : "GET"),
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: req,
    }),
    config.timeout,
  ).then(res => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.json() as Promise<R>;
  });
}

/**
 * COCKROACH LABS ENDPOINTS
 */

export function versionCheck(
  request: VersionCheckRequest,
  timeout?: moment.Duration,
): Promise<VersionList> {
  return timeoutFetch(
    `${COCKROACHLABS_ADDR}/api/clusters/updates?uuid=${request.clusterID}&version=${request.buildtag}`,
    null,
    { timeout },
  );
}
