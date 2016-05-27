/// <reference path="../../typings/main.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "isomorphic-fetch";
import * as protos from "../js/protos";
import * as _ from "lodash";

let server = protos.cockroach.server;
let ts = protos.cockroach.ts;

type DatabasesRequest = cockroach.server.DatabasesRequest;
type DatabasesResponseMessage = cockroach.server.DatabasesResponseMessage;

type DatabaseDetailsRequest = cockroach.server.DatabaseDetailsRequest;
type DatabaseDetailsResponseMessage = cockroach.server.DatabaseDetailsResponseMessage;

type TableDetailsRequest = cockroach.server.TableDetailsRequest;
type TableDetailsResponseMessage = cockroach.server.TableDetailsResponseMessage;

type EventsRequest = cockroach.server.EventsRequest;
type EventsResponseMessage = cockroach.server.EventsResponseMessage;

type NodesRequest = cockroach.server.NodesRequest;
type NodesResponseMessage = cockroach.server.NodesResponseMessage;

type GetUIDataRequest = cockroach.server.GetUIDataRequest;
type GetUIDataResponseMessage = cockroach.server.GetUIDataResponseMessage;

type SetUIDataRequestMessage = cockroach.server.SetUIDataRequestMessage;
type SetUIDataResponseMessage = cockroach.server.SetUIDataResponseMessage;

type TimeSeriesQueryRequestMessage = cockroach.ts.TimeSeriesQueryRequestMessage;
type TimeSeriesQueryResponseMessage = cockroach.ts.TimeSeriesQueryResponseMessage;

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
//
// This function is intended for use with generated protocol buffers. In
// particular, TResponse and TResponseMessage are generated interfaces which
// are implemented by the JSON representation of the protocol buffer and the
// JavaScript protocol buffer object itself, respectively. builder is an
// interface implemented by the builder objects provided at runtime by
// protobuf.js.
//
// TODO(tamird): note that most of builder's surface is currently unused - it
// is present for reasons of documentation and future use. In the future,
// makeFetch will communicate with the server using serialized protocol buffers
// in production mode, and using JSON in development mode. This paragraph will
// be removed when that behaviour is implemented.
function makeFetch<TRequestMesage extends {
  encodeJSON(): string
  toArrayBuffer(): ArrayBuffer
}, TResponse, TResponseMessage, TResponseMessageBuilder extends {
  new (json: TResponse): TResponseMessage
  decode(buffer: ArrayBuffer): TResponseMessage
  decode(buffer: ByteBuffer): TResponseMessage
  decode64(buffer: string): TResponseMessage
}>(builder: TResponseMessageBuilder, url: string): (req: TRequestMesage) => Promise<TResponseMessage> {
  return (req) => timeout(fetch(url, {
    method: req ? "POST" : "GET",
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
    body: req ? req.encodeJSON() : undefined,
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
export function getDatabaseList(req: DatabasesRequest = {}): Promise<DatabasesResponseMessage> {
  return makeFetch(server.DatabasesResponse, `${API_PREFIX}/databases`)(null);
}

// getDatabaseDetails gets details for a specific database
export function getDatabaseDetails(req: DatabaseDetailsRequest): Promise<DatabaseDetailsResponseMessage> {
  return makeFetch(server.DatabaseDetailsResponse, `${API_PREFIX}/databases/${req.database}`)(null);
}

// getTableDetails gets details for a specific table
export function getTableDetails(req: TableDetailsRequest): Promise<TableDetailsResponseMessage> {
  return makeFetch(server.TableDetailsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}`)(null);
}

// getUIData gets UI data
export function getUIData(req: GetUIDataRequest): Promise<GetUIDataResponseMessage> {
  let queryString = _.map(req.keys, (key) => "keys=" + encodeURIComponent(key)).join("&");
  return makeFetch(server.GetUIDataResponse, `${API_PREFIX}/uidata?${queryString}`)(null);
}

// setUIData sets UI data
export function setUIData(req: SetUIDataRequestMessage): Promise<SetUIDataResponseMessage> {
  return makeFetch(server.SetUIDataResponse, `${API_PREFIX}/uidata`)(req);
}

// getEvents gets event data
export function getEvents(req: EventsRequest = {}): Promise<EventsResponseMessage> {
  let queryString = propsToQueryString(_.pick(req, ["type", "target_id"]));
  return makeFetch(server.EventsResponse, `${API_PREFIX}/events?${queryString}`)(null);
}

// getNodes gets node data
export function getNodes(req: NodesRequest = {}): Promise<NodesResponseMessage> {
  return makeFetch(server.DatabasesResponse, `/_status/nodes`)(null);
}

// queryTimeSeries queries for time series data
export function queryTimeSeries(req: TimeSeriesQueryRequestMessage): Promise<TimeSeriesQueryResponseMessage> {
  return makeFetch(ts.TimeSeriesQueryResponse, `/ts/query`)(req);
}
