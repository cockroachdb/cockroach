/// <reference path="../../typings/main.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "isomorphic-fetch";
import * as protos from "../js/protos";
import * as _ from "lodash";

let server = protos.cockroach.server;

type DatabasesRequest = cockroach.server.DatabasesRequest;
type DatabasesResponseMessage = cockroach.server.DatabasesResponseMessage;

type DatabaseDetailsRequest = cockroach.server.DatabaseDetailsRequest;
type DatabaseDetailsResponseMessage = cockroach.server.DatabaseDetailsResponseMessage;

type TableDetailsRequest = cockroach.server.TableDetailsRequest;
type TableDetailsResponseMessage = cockroach.server.TableDetailsResponseMessage;

type EventsRequest = cockroach.server.EventsRequest;
type EventsResponseMessage = cockroach.server.EventsResponseMessage;

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
function makeFetch<TResponse, TResponseMessage>(
  builder: {
    new (json: TResponse): TResponseMessage
    decode(buffer: ArrayBuffer): TResponseMessage
    decode(buffer: ByteBuffer): TResponseMessage
    decode64(buffer: string): TResponseMessage
  },
  url: string,
  method = "GET",
): Promise<TResponseMessage> {
  return timeout(fetch(url, {
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
    method,
  })).then((res) => res.json<TResponse>()).then((json) => new builder(json));
}
// propsToQueryString is a helper function that converts a set of object
// properties to a query string
// - keys with null or undefined values will be skipped
// - non-string values will be toString'd
export function propsToQueryString(props: any) {
  return _.compact(_.map(props, (v: any, k: string) => !_.isNull(v) && !_.isUndefined(v) ? `${encodeURIComponent(k)}=${encodeURIComponent(v.toString())}` : null)).join("&");
}
/**
 * ENDPOINTS
 */

// getDatabaseList gets a list of all database names
export function getDatabaseList(): Promise<DatabasesResponseMessage> {
  return makeFetch(server.DatabasesResponse, `${API_PREFIX}/databases`);
}

// getDatabaseDetails gets details for a specific database
export function getDatabaseDetails(req: DatabaseDetailsRequest): Promise<DatabaseDetailsResponseMessage> {
  return makeFetch(server.DatabaseDetailsResponse, `${API_PREFIX}/databases/${req.database}`);
}

// getTableDetails gets details for a specific table
export function getTableDetails(req: TableDetailsRequest): Promise<TableDetailsResponseMessage> {
  return makeFetch(server.TableDetailsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}`);
}

// getEvents gets event data
export function getEvents(req: EventsRequest = {}): Promise<EventsResponseMessage> {
  let queryString = propsToQueryString(_.pick(req, ["type", "target_id"]));
  return makeFetch(server.EventsResponse, `${API_PREFIX}/events?${queryString}`);
}
