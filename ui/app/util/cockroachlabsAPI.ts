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

/**
 * COCKROACH LABS ENDPOINTS
 */

export function versionCheck(clusterID: string, buildtag: string): Promise<VersionList> {
  return timeoutFetch(`${COCKROACHLABS_ADDR}/api/clusters/updates?uuid=${clusterID}&version=${buildtag}`);
}
