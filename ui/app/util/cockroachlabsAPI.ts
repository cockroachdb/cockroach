/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "whatwg-fetch";
import moment = require("moment");

import { VersionList } from "../interfaces/cockroachlabs";
import { withTimeout } from "./api";

export const COCKROACHLABS_ADDR = "https://register.cockroachdb.com";

// TODO(maxlang): might be possible to consolidate with Fetch in api.ts
function timeoutFetch<T extends BodyInit, R>(url: string, req?: T, timeout?: moment.Duration): Promise<R> {
  return withTimeout(
    fetch(url, {
      method: req ? "POST" : "GET",
      headers: {
        "Accept": "application/json",
        "Content-Type": "application/json",
      },
      body: req,
    }),
    timeout
  ).then((res) => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.json();
  });
}

export interface VersionCheckRequest {
  clusterID: string;
  buildtag: string;
}

/**
 * COCKROACH LABS ENDPOINTS
 */

export function versionCheck(request: VersionCheckRequest, timeout?: moment.Duration): Promise<VersionList> {
  return timeoutFetch(`${COCKROACHLABS_ADDR}/api/clusters/updates?uuid=${request.clusterID}&version=${request.buildtag}`, null, timeout);
}
