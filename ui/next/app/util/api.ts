/// <reference path="../../typings/main.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "isomorphic-fetch";

const API_PREFIX = "/_admin/v1";
const TIMEOUT = 10000; // 10 seconds

type DatabasesResponse = cockroach.server.DatabasesResponse;

// Inspired by https://github.com/github/fetch/issues/175
function timeout<T>(promise: Promise<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    setTimeout(() => reject(new Error(`Promise timed out after ${TIMEOUT} ms`)), TIMEOUT);
    promise.then(resolve, reject);
  });
}

function generateGetEndpoint<TRequest, TResponse>(endpoint: string, toUrl?: (params: TRequest) => string) {
  return function (params?: TRequest) {
    return timeout(fetch(`${API_PREFIX}/${endpoint}${toUrl ? "/" + toUrl(params) : ""}`, {
      headers: {
        "Accept": "application/json",
        "Content-Type": "application/json",
      },
    }))
      .then((r) => r.json<TResponse>());
  };
}

/**
 * ENDPOINTS
 */

// Database List
export let getDatabaseList = generateGetEndpoint<void, cockroach.server.DatabasesResponse>("databases");
