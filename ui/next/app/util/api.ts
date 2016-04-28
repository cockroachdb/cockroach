/// <reference path="../../typings/main.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "isomorphic-fetch";

export const API_PREFIX = "/_admin/v1";
let TIMEOUT = 10000; // 10 seconds

export function setFetchTimeout(v: number) {
  TIMEOUT = v;
};

type DatabasesResponse = cockroach.server.DatabasesResponse;

// Inspired by https://github.com/github/fetch/issues/175
// wraps a promise in a timeout
function timeout<T>(promise: Promise<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    setTimeout(() => reject(new Error(`Promise timed out after ${TIMEOUT} ms`)), TIMEOUT);
    promise.then(resolve, reject);
  });
}

/**
 * generateGetEndpoint generates a new get endpoint
 * @param endpoint is the API endpoing to hit
 * @param toUrl is a function that takes a request and transforms it into a string which will be appended to the URL
 * @return returns a function that runs fetch and returns a promise
 */
function generateGetEndpoint<TResponse>(endpoint: string) {
  return function () {
    return timeout(fetch(`${API_PREFIX}/${endpoint}`, {
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

// getDatabaseList returns DatabasesResponse containing a list of all database names as strings
export let getDatabaseList = generateGetEndpoint<cockroach.server.DatabasesResponse>("databases");
