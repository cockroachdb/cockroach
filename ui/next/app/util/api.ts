/// <reference path="../../typings/index.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import "isomorphic-fetch";
import * as _ from "lodash";

import * as protos from "../js/protos";

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

type RaftDebugRequest = cockroach.server.RaftDebugRequest;
type RaftDebugResponseMessage = cockroach.server.RaftDebugResponseMessage;

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

// Fetch is a wrapped around fetch that provides timeout and protocol buffer
// marshalling and unmarshalling.
//
// This function is intended for use with generated protocol buffers. In
// particular, TResponse is a generated interface that describes the JSON
// representation of the response, while TRequestMessage and TResponseMessage
// are generated interfaces which are implemented by the protocol buffer
// objects themselves. TResponseMessageBuilder is an interface implemented by
// the builder objects provided at runtime by protobuf.js.
//
// TODO(tamird): note that most of TResponseMessageBuilder's surface is
// currently unused - it is present for reasons of documentation and future
// use. In the future, Fetch will communicate with the server using serialized
// protocol buffers in production mode, and using JSON in development mode.
// This paragraph will be removed when that behaviour is implemented.
function Fetch<TRequestMessage extends {
  encodeJSON(): string
  toArrayBuffer(): ArrayBuffer
}, TResponse, TResponseMessage, TResponseMessageBuilder extends {
  new (json: TResponse): TResponseMessage
  decode(buffer: ArrayBuffer): TResponseMessage
  decode(buffer: ByteBuffer): TResponseMessage
  decode64(buffer: string): TResponseMessage
}>(builder: TResponseMessageBuilder, url: string, req: TRequestMessage = null): Promise<TResponseMessage> {
  return timeout(fetch(url, {
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
  return Fetch(server.DatabasesResponse, `${API_PREFIX}/databases`);
}

// getDatabaseDetails gets details for a specific database
export function getDatabaseDetails(req: DatabaseDetailsRequest): Promise<DatabaseDetailsResponseMessage> {
  return Fetch(server.DatabaseDetailsResponse, `${API_PREFIX}/databases/${req.database}`);
}

// getTableDetails gets details for a specific table
export function getTableDetails(req: TableDetailsRequest): Promise<TableDetailsResponseMessage> {
  return Fetch(server.TableDetailsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}`);
}

// getUIData gets UI data
export function getUIData(req: GetUIDataRequest): Promise<GetUIDataResponseMessage> {
  let queryString = _.map(req.keys, (key) => "keys=" + encodeURIComponent(key)).join("&");
  return Fetch(server.GetUIDataResponse, `${API_PREFIX}/uidata?${queryString}`);
}

// setUIData sets UI data
export function setUIData(req: SetUIDataRequestMessage): Promise<SetUIDataResponseMessage> {
  return Fetch(server.SetUIDataResponse, `${API_PREFIX}/uidata`, req);
}

// getEvents gets event data
export function getEvents(req: EventsRequest = {}): Promise<EventsResponseMessage> {
  let queryString = propsToQueryString(_.pick(req, ["type", "target_id"]));
  return Fetch(server.EventsResponse, `${API_PREFIX}/events?${queryString}`);
}

// getNodes gets node data
export function getNodes(req: NodesRequest = {}): Promise<NodesResponseMessage> {
  return Fetch(server.NodesResponse, `/_status/nodes`);
}

// raftDebug returns raft debug information.
export function raftDebug(req: RaftDebugRequest = {}): Promise<RaftDebugResponseMessage> {
  return Fetch(server.RaftDebugResponse, `/_status/raft`);
}

// queryTimeSeries queries for time series data
export function queryTimeSeries(req: TimeSeriesQueryRequestMessage): Promise<TimeSeriesQueryResponseMessage> {
  return Fetch(ts.TimeSeriesQueryResponse, `/ts/query`, req);
}
