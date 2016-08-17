/// <reference path="../../typings/index.d.ts" />

/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import * as _ from "lodash";
import "whatwg-fetch";

import * as protos from "../js/protos";

let serverpb = protos.cockroach.server.serverpb;
let ts = protos.cockroach.ts.tspb;

export type DatabasesResponseMessage = cockroach.server.serverpb.DatabasesResponseMessage;

export type DatabaseDetailsRequest = cockroach.server.serverpb.DatabaseDetailsRequest;
export type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;

export type TableDetailsRequest = cockroach.server.serverpb.TableDetailsRequest;
export type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;

export type EventsRequest = cockroach.server.serverpb.EventsRequest;
export type EventsResponseMessage = cockroach.server.serverpb.EventsResponseMessage;

export type NodesResponseMessage = cockroach.server.serverpb.NodesResponseMessage;

export type GetUIDataRequest = cockroach.server.serverpb.GetUIDataRequest;
export type GetUIDataResponseMessage = cockroach.server.serverpb.GetUIDataResponseMessage;

export type SetUIDataRequestMessage = cockroach.server.serverpb.SetUIDataRequestMessage;
export type SetUIDataResponseMessage = cockroach.server.serverpb.SetUIDataResponseMessage;

export type RaftDebugResponseMessage = cockroach.server.serverpb.RaftDebugResponseMessage;

export type TimeSeriesQueryRequestMessage = cockroach.ts.tspb.TimeSeriesQueryRequestMessage;
export type TimeSeriesQueryResponseMessage = cockroach.ts.tspb.TimeSeriesQueryResponseMessage;

export type HealthResponseMessage = cockroach.server.serverpb.HealthResponseMessage;

export type ClusterResponseMessage = cockroach.server.serverpb.ClusterResponseMessage;

export type TableStatsRequest = cockroach.server.serverpb.TableStatsRequest;
export type TableStatsResponseMessage = cockroach.server.serverpb.TableStatsResponseMessage;

export type LogsRequest = cockroach.server.serverpb.LogsRequest;
export type LogEntriesResponseMessage = cockroach.server.serverpb.LogEntriesResponseMessage;

// API constants

export const API_PREFIX = "/_admin/v1";
export const STATUS_PREFIX = "/_status";
let TIMEOUT = 30000; // 30 seconds in milliseconds.

/**
 * HELPER FUNCTIONS
 */

// Changes the fetch timeout; only used for testing.
export function setFetchTimeout(v: number) {
  TIMEOUT = v;
};

// Inspired by https://github.com/github/fetch/issues/175
// wraps a promise in a timeout
export function timeout<T>(promise: Promise<T>): Promise<T> {
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
      "Accept": "application/x-protobuf",
      "Content-Type": "application/x-protobuf",
      "Grpc-Timeout": TIMEOUT + "m", // m is milliseconds
    },
    body: req ? req.toArrayBuffer() : undefined,
  })).then((res) => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.arrayBuffer().then((buffer) => builder.decode(buffer));
  });
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
  return Fetch(serverpb.DatabasesResponse, `${API_PREFIX}/databases`);
}

// getDatabaseDetails gets details for a specific database
export function getDatabaseDetails(req: DatabaseDetailsRequest): Promise<DatabaseDetailsResponseMessage> {
  return Fetch(serverpb.DatabaseDetailsResponse, `${API_PREFIX}/databases/${req.database}`);
}

// getTableDetails gets details for a specific table
export function getTableDetails(req: TableDetailsRequest): Promise<TableDetailsResponseMessage> {
  return Fetch(serverpb.TableDetailsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}`);
}

// getUIData gets UI data
export function getUIData(req: GetUIDataRequest): Promise<GetUIDataResponseMessage> {
  let queryString = _.map(req.keys, (key) => "keys=" + encodeURIComponent(key)).join("&");
  return Fetch(serverpb.GetUIDataResponse, `${API_PREFIX}/uidata?${queryString}`);
}

// setUIData sets UI data
export function setUIData(req: SetUIDataRequestMessage): Promise<SetUIDataResponseMessage> {
  return Fetch(serverpb.SetUIDataResponse, `${API_PREFIX}/uidata`, req);
}

// getEvents gets event data
export function getEvents(req: EventsRequest = {}): Promise<EventsResponseMessage> {
  let queryString = propsToQueryString(_.pick(req, ["type", "target_id"]));
  return Fetch(serverpb.EventsResponse, `${API_PREFIX}/events?${queryString}`);
}

// getNodes gets node data
export function getNodes(): Promise<NodesResponseMessage> {
  return Fetch(serverpb.NodesResponse, `/_status/nodes`);
}

// raftDebug returns raft debug information.
export function raftDebug(): Promise<RaftDebugResponseMessage> {
  return Fetch(serverpb.RaftDebugResponse, `/_status/raft`);
}

// queryTimeSeries queries for time series data
export function queryTimeSeries(req: TimeSeriesQueryRequestMessage): Promise<TimeSeriesQueryResponseMessage> {
  return Fetch(ts.TimeSeriesQueryResponse, `/ts/query`, req);
}

// getHealth gets health data
export function getHealth(): Promise<HealthResponseMessage> {
  return Fetch(serverpb.HealthResponse, `${API_PREFIX}/health`);
}

// getCluster gets info about the cluster
export function getCluster(): Promise<ClusterResponseMessage> {
  return Fetch(serverpb.ClusterResponse, `${API_PREFIX}/cluster`);
}

// getTableStats gets details stats about the current table
export function getTableStats(req: TableStatsRequest): Promise<TableStatsResponseMessage> {
  return Fetch(serverpb.TableStatsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}/stats`);
}

// TODO (maxlang): add filtering
// TODO: switch to using Fetch once #8569 is fixed
// getLogs gets the logs for a specific node
export function getLogs(req: LogsRequest): Promise<LogEntriesResponseMessage> {
  return timeout(fetch(`${STATUS_PREFIX}/logs/${req.node_id}`, {
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
  })).then((res) => res.json().then((json) => new protos.cockroach.server.serverpb.LogEntriesResponse(json)));
}
