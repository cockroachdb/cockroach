/**
 * This module contains all the REST endpoints for communicating with the admin UI.
 */

import _ from "lodash";
import "whatwg-fetch"; // needed for jsdom?
import moment from "moment";

import * as protos from "../js/protos";

export type DatabasesRequestMessage = protos.cockroach.server.serverpb.DatabasesRequest;
export type DatabasesResponseMessage = protos.cockroach.server.serverpb.DatabasesResponse;

export type DatabaseDetailsRequestMessage = protos.cockroach.server.serverpb.DatabaseDetailsRequest;
export type DatabaseDetailsResponseMessage = protos.cockroach.server.serverpb.DatabaseDetailsResponse;

export type TableDetailsRequestMessage = protos.cockroach.server.serverpb.TableDetailsRequest;
export type TableDetailsResponseMessage = protos.cockroach.server.serverpb.TableDetailsResponse;

export type EventsRequestMessage = protos.cockroach.server.serverpb.EventsRequest;
export type EventsResponseMessage = protos.cockroach.server.serverpb.EventsResponse;

export type NodesRequestMessage = protos.cockroach.server.serverpb.NodesRequest;
export type NodesResponseMessage = protos.cockroach.server.serverpb.NodesResponse;

export type GetUIDataRequestMessage = protos.cockroach.server.serverpb.GetUIDataRequest;
export type GetUIDataResponseMessage = protos.cockroach.server.serverpb.GetUIDataResponse;

export type SetUIDataRequestMessage = protos.cockroach.server.serverpb.SetUIDataRequest;
export type SetUIDataResponseMessage = protos.cockroach.server.serverpb.SetUIDataResponse;

export type RaftDebugRequestMessage = protos.cockroach.server.serverpb.RaftDebugRequest;
export type RaftDebugResponseMessage = protos.cockroach.server.serverpb.RaftDebugResponse;

export type TimeSeriesQueryRequestMessage = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;
export type TimeSeriesQueryResponseMessage = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

export type HealthRequestMessage = protos.cockroach.server.serverpb.HealthRequest;
export type HealthResponseMessage = protos.cockroach.server.serverpb.HealthResponse;

export type ClusterRequestMessage = protos.cockroach.server.serverpb.ClusterRequest;
export type ClusterResponseMessage = protos.cockroach.server.serverpb.ClusterResponse;

export type TableStatsRequestMessage = protos.cockroach.server.serverpb.TableStatsRequest;
export type TableStatsResponseMessage = protos.cockroach.server.serverpb.TableStatsResponse;

export type LogsRequestMessage = protos.cockroach.server.serverpb.LogsRequest;
export type LogEntriesResponseMessage = protos.cockroach.server.serverpb.LogEntriesResponse;

export type LivenessRequestMessage = protos.cockroach.server.serverpb.LivenessRequest;
export type LivenessResponseMessage = protos.cockroach.server.serverpb.LivenessResponse;

// API constants

export const API_PREFIX = "_admin/v1";
export const STATUS_PREFIX = "_status";

// HELPER FUNCTIONS

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

interface TRequest {
  constructor: {
    encode(message: TRequest, writer?: protobuf.Writer): protobuf.Writer;
  };
  toObject(): { [k: string]: any };
}

export function toArrayBuffer(encodedRequest: Uint8Array): ArrayBuffer {
  return encodedRequest.buffer.slice(encodedRequest.byteOffset, encodedRequest.byteOffset + encodedRequest.byteLength);
}

// timeoutFetch is a wrapper around fetch that provides timeout and protocol
// buffer marshalling and unmarshalling.
//
// This function is intended for use with generated protocol buffers. In
// particular, TResponse$Properties is a generated interface that describes the JSON
// representation of the response, while TRequest and TResponse
// are generated interfaces which are implemented by the protocol buffer
// objects themselves. TResponseBuilder is an interface implemented by
// the builder objects provided at runtime by protobuf.js.
function timeoutFetch<TResponse$Properties, TResponse, TResponseBuilder extends {
  new (properties?: TResponse$Properties): TResponse
  encode(message: TResponse$Properties, writer?: protobuf.Writer): protobuf.Writer
  decode(reader: (protobuf.Reader|Uint8Array), length?: number): TResponse;
}>(builder: TResponseBuilder, url: string, req: TRequest = null, timeout: moment.Duration = moment.duration(30, "s")): Promise<TResponse> {
  const params: RequestInit = {
    headers: {
      "Accept": "application/x-protobuf",
      "Content-Type": "application/x-protobuf",
      "Grpc-Timeout": timeout ? timeout.asMilliseconds() + "m" : undefined,
    },
    credentials: "same-origin",
  };

  if (req) {
    const encodedRequest = req.constructor.encode(req).finish();
    params.method = "POST";
    params.body = toArrayBuffer(encodedRequest);
  }

  return withTimeout(fetch(url, params), timeout).then((res) => {
    if (!res.ok) {
      throw Error(res.statusText);
    }
    return res.arrayBuffer().then((buffer) => builder.decode(new Uint8Array(buffer)));
  });
}

export type APIRequestFn<TRequest, TResponse> = (req: TRequest, timeout?: moment.Duration) => Promise<TResponse>;

// propsToQueryString is a helper function that converts a set of object
// properties to a query string
// - keys with null or undefined values will be skipped
// - non-string values will be toString'd
export function propsToQueryString(props: { [k: string]: any }) {
  return _.compact(_.map(props, (v: any, k: string) => !_.isNull(v) && !_.isUndefined(v) ? `${encodeURIComponent(k)}=${encodeURIComponent(v.toString())}` : null)).join("&");
}
/**
 * ENDPOINTS
 */

const serverpb = protos.cockroach.server.serverpb;
const tspb = protos.cockroach.ts.tspb;

// getDatabaseList gets a list of all database names
export function getDatabaseList(_req: DatabasesRequestMessage, timeout?: moment.Duration): Promise<DatabasesResponseMessage> {
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
  return timeoutFetch(serverpb.SetUIDataResponse, `${API_PREFIX}/uidata`, req as any, timeout);
}

// getEvents gets event data
export function getEvents(req: EventsRequestMessage, timeout?: moment.Duration): Promise<EventsResponseMessage> {
  let queryString = propsToQueryString(_.pick(req, ["type", "target_id"]));
  return timeoutFetch(serverpb.EventsResponse, `${API_PREFIX}/events?${queryString}`, null, timeout);
}

// getNodes gets node data
export function getNodes(_req: NodesRequestMessage, timeout?: moment.Duration): Promise<NodesResponseMessage> {
  return timeoutFetch(serverpb.NodesResponse, `${STATUS_PREFIX}/nodes`, null, timeout);
}

export function raftDebug(_req: RaftDebugRequestMessage): Promise<RaftDebugResponseMessage> {
  // NB: raftDebug intentionally does not pass a timeout through.
  return timeoutFetch(serverpb.RaftDebugResponse, `${STATUS_PREFIX}/raft`);
}

// queryTimeSeries queries for time series data
export function queryTimeSeries(req: TimeSeriesQueryRequestMessage, timeout?: moment.Duration): Promise<TimeSeriesQueryResponseMessage> {
  return timeoutFetch(tspb.TimeSeriesQueryResponse, `ts/query`, req as any, timeout);
}

// getHealth gets health data
export function getHealth(_req: HealthRequestMessage, timeout?: moment.Duration): Promise<HealthResponseMessage> {
  return timeoutFetch(serverpb.HealthResponse, `${API_PREFIX}/health`, null, timeout);
}

// getCluster gets info about the cluster
export function getCluster(_req: ClusterRequestMessage, timeout?: moment.Duration): Promise<ClusterResponseMessage> {
  return timeoutFetch(serverpb.ClusterResponse, `${API_PREFIX}/cluster`, null, timeout);
}

// getTableStats gets details stats about the current table
export function getTableStats(req: TableStatsRequestMessage, timeout?: moment.Duration): Promise<TableStatsResponseMessage> {
  return timeoutFetch(serverpb.TableStatsResponse, `${API_PREFIX}/databases/${req.database}/tables/${req.table}/stats`, null, timeout);
}

// TODO (maxlang): add filtering
// getLogs gets the logs for a specific node
export function getLogs(req: LogsRequestMessage, timeout?: moment.Duration): Promise<LogEntriesResponseMessage> {
  return timeoutFetch(serverpb.LogEntriesResponse, `${STATUS_PREFIX}/logs/${req.node_id}`, null, timeout);
}

// getLiveness gets cluster liveness information from the current node.
export function getLiveness(_: LivenessRequestMessage, timeout?: moment.Duration): Promise<LivenessResponseMessage> {
  return timeoutFetch(serverpb.LivenessResponse, `${API_PREFIX}/liveness`, null, timeout);
}
