/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "whatwg-fetch";
import { VersionList } from "../interfaces/cockroachlabs";
import { timeout } from "./api";

export const COCKROACHLABS_ADDR = "https://register.cockroachdb.com";

// TODO(maxlang): might be possible to consolidate with Fetch in api.ts
function timeoutFetch<T extends BodyInit, R>(url: string, req?: T): Promise<R> {
  return timeout(fetch(url, {
    method: req ? "POST" : "GET",
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
    body: req,
  })).then((res) => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.json<R>();
  });
}

export interface VersionCheckRequest {
  clusterID: string;
  buildtag: string;
}

/**
 * COCKROACH LABS ENDPOINTS
 */

export function versionCheck(request: VersionCheckRequest): Promise<VersionList> {
  return timeoutFetch(`${COCKROACHLABS_ADDR}/api/clusters/updates?uuid=${request.clusterID}&version=${request.buildtag}`);
}
