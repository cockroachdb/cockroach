/// <reference path="../../typings/main.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "isomorphic-fetch";
import * as protos from "../js/protos";

let server = protos.cockroach.server;

type DatabasesRequest = cockroach.server.DatabasesRequest;
type DatabasesResponse = cockroach.server.DatabasesResponse;

type DatabaseDetailsRequest = cockroach.server.DatabaseDetailsRequest;
type DatabaseDetailsResponse = cockroach.server.DatabaseDetailsResponse;

type TableDetailsRequest = cockroach.server.TableDetailsRequest;
type TableDetailsResponse = cockroach.server.TableDetailsResponse;

export const API_PREFIX = "/_admin/v1";
let TIMEOUT = 10000; // 10 seconds

export function setFetchTimeout(v: number) {
  TIMEOUT = v;
};

/**
 * HELPER FUNCTIONS
 */

// Inspired by https://github.com/github/fetch/issues/175
// wraps a promise in a timeout
function timeout<T>(promise: Promise<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    setTimeout(() => reject(new Error(`Promise timed out after ${TIMEOUT} ms`)), TIMEOUT);
    promise.then(resolve, reject);
  });
}

// makeFetch generates a new fetch request to the given url
function makeFetch(url: string) {
  return timeout(fetch(url, {
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
  }));
}

/**
 * ENDPOINTS
 */

// getDatabaseList returns DatabasesResponse containing a list of all database names as strings
export function getDatabaseList() {
  return makeFetch(`${API_PREFIX}/databases`)
    .then((res) => res.json<DatabasesResponse>())
    .then((res) => new server.DatabasesResponse(res));
}

// getDatabaseDetails gets details for a specific database
export function getDatabaseDetails(req: DatabaseDetailsRequest) {
  return makeFetch(`${API_PREFIX}/databases/${req.database}`)
    .then((res) => res.json<DatabaseDetailsResponse>())
    .then((res) => new server.DatabaseDetailsResponse(res));
}

// getTableDetails gets details for a specific table
export function getTableDetails(req: TableDetailsRequest) {
  return makeFetch(`${API_PREFIX}/databases/${req.database}/tables/${req.table}`)
    .then((res) => res.json<TableDetailsResponse>())
    .then((res) => new server.TableDetailsResponse(res));
}
