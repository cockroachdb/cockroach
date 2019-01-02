// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import moment from "moment";

import {
  VersionList, VersionCheckRequest,
} from "src/interfaces/cockroachlabs";
import { withTimeout } from "./api";

export const COCKROACHLABS_ADDR = "https://register.cockroachdb.com";

interface FetchConfig {
  method?: string;
  timeout?: moment.Duration;
}

// TODO(maxlang): might be possible to consolidate with Fetch in api.ts
function timeoutFetch<T extends BodyInit, R>(url: string, req?: T, config: FetchConfig = {}): Promise<R> {
  return withTimeout(
    fetch(url, {
      method: config.method || (req ? "POST" : "GET"),
      headers: {
        "Accept": "application/json",
        "Content-Type": "application/json",
      },
      body: req,
    }),
    config.timeout,
  ).then((res) => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.json() as Promise<R>;
  });
}

/**
 * COCKROACH LABS ENDPOINTS
 */

export function versionCheck(request: VersionCheckRequest, timeout?: moment.Duration): Promise<VersionList> {
  return timeoutFetch(`${COCKROACHLABS_ADDR}/api/clusters/updates?uuid=${request.clusterID}&version=${request.buildtag}`, null, { timeout });
}
