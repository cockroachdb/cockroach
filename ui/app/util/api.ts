/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import * as _ from "lodash";
import "whatwg-fetch";
import moment = require("moment");

import * as protos from "../js/protos";

let serverpb = protos.cockroach.server.serverpb;
let ts = protos.cockroach.ts.tspb;

export type DatabasesRequestMessage = cockroach.server.serverpb.DatabasesRequestMessage;
export type DatabasesResponseMessage = cockroach.server.serverpb.DatabasesResponseMessage;

export type DatabaseDetailsRequestMessage = cockroach.server.serverpb.DatabaseDetailsRequestMessage;
export type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;

export type TableDetailsRequestMessage = cockroach.server.serverpb.TableDetailsRequestMessage;
export type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;

export type EventsRequestMessage = cockroach.server.serverpb.EventsRequestMessage;
export type EventsResponseMessage = cockroach.server.serverpb.EventsResponseMessage;

export type NodesRequestMessage = cockroach.server.serverpb.NodesRequestMessage;
export type NodesResponseMessage = cockroach.server.serverpb.NodesResponseMessage;

export type GetUIDataRequestMessage = cockroach.server.serverpb.GetUIDataRequestMessage;
export type GetUIDataResponseMessage = cockroach.server.serverpb.GetUIDataResponseMessage;

export type SetUIDataRequestMessage = cockroach.server.serverpb.SetUIDataRequestMessage;
export type SetUIDataResponseMessage = cockroach.server.serverpb.SetUIDataResponseMessage;

export type RaftDebugRequestMessage = cockroach.server.serverpb.RaftDebugRequestMessage;
export type RaftDebugResponseMessage = cockroach.server.serverpb.RaftDebugResponseMessage;

export type TimeSeriesQueryRequestMessage = cockroach.ts.tspb.TimeSeriesQueryRequestMessage;
export type TimeSeriesQueryResponseMessage = cockroach.ts.tspb.TimeSeriesQueryResponseMessage;

export type HealthRequestMessage = cockroach.server.serverpb.HealthRequestMessage;
export type HealthResponseMessage = cockroach.server.serverpb.HealthResponseMessage;

export type ClusterRequestMessage = cockroach.server.serverpb.ClusterRequestMessage;
export type ClusterResponseMessage = cockroach.server.serverpb.ClusterResponseMessage;

export type TableStatsRequestMessage = cockroach.server.serverpb.TableStatsRequestMessage;
export type TableStatsResponseMessage = cockroach.server.serverpb.TableStatsResponseMessage;

export const API_PREFIX = "/_admin/v1";

/**
 * HELPER FUNCTIONS
 */

// Inspired by https://github.com/github/fetch/issues/175
//
// withTimeout wraps a promise in a timeout.
export function withTimeout<T>(promise: Promise<T>, timeout?: moment.Duration): Promise<T> {
  if (timeout) {
    return new Promise<T>((resolve, reject) => {
      setTimeout(() => reject(new Error(`Promise timed out after ${timeout.asMilliseconds()} ms`)), timeout.asMilliseconds());
      promise.then(resolve, reject);
    });
  } else {
    return promise;
  }
}

interface TRequestMessage {
  encodeJSON(): string;
  toArrayBuffer(): ArrayBuffer;
}

// timeoutFetch is a wrapper around fetch that provides timeout and protocol
// buffer marshalling and unmarshalling.
//
// This function is intended for use with generated protocol buffers. In
// particular, TResponse is a generated interface that describes the JSON
// representation of the response, while TRequestMessage and TResponseMessage
// are generated interfaces which are implemented by the protocol buffer
// objects themselves. TResponseMessageBuilder is an interface implemented by
// the builder objects provided at runtime by protobuf.js.
function timeoutFetch<TResponse, TResponseMessage, TResponseMessageBuilder extends {
  new (json: TResponse): TResponseMessage
  decode(buffer: ArrayBuffer): TResponseMessage
  decode(buffer: ByteBuffer): TResponseMessage
  decode64(buffer: string): TResponseMessage
}>(builder: TResponseMessageBuilder, url: string, req: TRequestMessage = null, timeout: moment.Duration = moment.duration(30, "s")): Promise<TResponseMessage> {
  return withTimeout(
    fetch(url, {
      method: req ? "POST" : "GET",
      headers: {
        "Accept": "application/x-protobuf",
        "Content-Type": "application/x-protobuf",
        "Grpc-Timeout": timeout ? timeout.asMilliseconds() + "m" : undefined,
      },
      body: req ? req.toArrayBuffer() : undefined,
    }),
    timeout
   ).then((res) => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.arrayBuffer().then((buffer) => builder.decode(buffer));
  });
}

export type APIRequestFn<TRequestMessage, TResponseMessage> = (req: TRequestMessage, timeout?: moment.Duration) => Promise<TResponseMessage>

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
export function getDatabaseList(req: DatabasesRequestMessage, timeout?: moment.Duration): Promise<DatabasesResponseMessage> {
  return timeoutFetch(serverpb.DatabasesResponse, `${API_PREFIX}/databases`, null, timeout);
}

// getDatabaseDetails gets details for a specific database
export function getDatabaseDetails(req: DatabaseDetailsRequestMessage, timeout?: moment.Duration): Promise<DatabaseDetailsResponseMessage> {
  return timeoutFetch(serverpb.DatabaseDetailsResponse, `${API_PREFIX}/databases/${req.database}`, null, timeout);
}

// getTableDetails gets details for a specific table
export function getTableDetails(req: TableDetailsRequestMessage, timeout?: moment.Duration): Promise<TableDetailsResponseMessage> {
  return timeoutFetch(serverpb.TableDetailsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}`, null, timeout);
}

// getUIData gets UI data
export function getUIData(req: GetUIDataRequestMessage, timeout?: moment.Duration): Promise<GetUIDataResponseMessage> {
  let queryString = _.map(req.keys, (key) => "keys=" + encodeURIComponent(key)).join("&");
  return timeoutFetch(serverpb.GetUIDataResponse, `${API_PREFIX}/uidata?${queryString}`, null, timeout);
}

// setUIData sets UI data
export function setUIData(req: SetUIDataRequestMessage, timeout?: moment.Duration): Promise<SetUIDataResponseMessage> {
  return timeoutFetch(serverpb.SetUIDataResponse, `${API_PREFIX}/uidata`, req, timeout);
}

// getEvents gets event data
export function getEvents(req: EventsRequestMessage, timeout?: moment.Duration): Promise<EventsResponseMessage> {
  let queryString = propsToQueryString(_.pick(req, ["type", "target_id"]));
  return timeoutFetch(serverpb.EventsResponse, `${API_PREFIX}/events?${queryString}`, null, timeout);
}

// getNodes gets node data
export function getNodes(req: NodesRequestMessage, timeout?: moment.Duration): Promise<NodesResponseMessage> {
  return timeoutFetch(serverpb.NodesResponse, `/_status/nodes`, null, timeout);
}

export function raftDebug(req: RaftDebugRequestMessage): Promise<RaftDebugResponseMessage> {
  // NB: raftDebug intentionally does not pass a timeout through.
  return timeoutFetch(serverpb.RaftDebugResponse, `/_status/raft`);
}

// queryTimeSeries queries for time series data
export function queryTimeSeries(req: TimeSeriesQueryRequestMessage, timeout?: moment.Duration): Promise<TimeSeriesQueryResponseMessage> {
  return timeoutFetch(ts.TimeSeriesQueryResponse, `/ts/query`, req, timeout);
}

// getHealth gets health data
export function getHealth(req: HealthRequestMessage, timeout?: moment.Duration): Promise<HealthResponseMessage> {
  return timeoutFetch(serverpb.HealthResponse, `${API_PREFIX}/health`, null, timeout);
}

// getCluster gets info about the cluster
export function getCluster(req: ClusterRequestMessage, timeout?: moment.Duration): Promise<ClusterResponseMessage> {
  return timeoutFetch(serverpb.ClusterResponse, `${API_PREFIX}/cluster`, null, timeout);
}

// getTableStats gets details stats about the current table
export function getTableStats(req: TableStatsRequestMessage, timeout?: moment.Duration): Promise<TableStatsResponseMessage> {
  return timeoutFetch(serverpb.TableStatsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}/stats`, null, timeout);
}
