// source: models/proto.ts
// Author: Matt Tracy (matt@cockroachlabs.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/* tslint:disable:jsdoc-format */
module Models {
  "use strict";
  /**
   * The Proto package contains data interfaces which correspond to
   * protobuffer messages on the server; any message returned directly from
   * the server should be contained in this package.
   *
   * The interfaces in this package are currently maintained manually, but it
   * should be possible in the future to automatically generate these
   * structures.
   */
  export module Proto {
    /*****************************
     * roachpb/data.proto
     ****************************/

    /**
     * MVCCStats provides detailed information about currently stored data
     * in the engine.
     */
    export interface MVCCStats {
      live_bytes: number;
      key_bytes: number;
      val_bytes: number;
      intent_bytes: number;
      live_count: number;
      key_count: number;
      val_count: number;
      intent_count: number;
      intent_age: number;
      gc_bytes_age: number;
      sys_bytes: number;
      sys_count: number;
      last_update_nanos: number;
    }

    /**
     * Create a new object which implements MVCCStats interface, with zero
     * values.
     */
    export function NewMVCCStats(): MVCCStats {
      return {
        live_bytes: 0,
        key_bytes: 0,
        val_bytes: 0,
        intent_bytes: 0,
        live_count: 0,
        key_count: 0,
        val_count: 0,
        intent_count: 0,
        intent_age: 0,
        gc_bytes_age: 0,
        sys_bytes: 0,
        sys_count: 0,
        last_update_nanos: 0,
      };
    }

    /**
     * AccumulateMVCCStats accumulates values from a source MVCCStats into
     * the values of a destination MVCCStats value.
     */
    export function AccumulateMVCCStats(dest: Proto.MVCCStats, src: Proto.MVCCStats): void {
      dest.live_bytes += src.live_bytes;
      dest.key_bytes += src.key_bytes;
      dest.val_bytes += src.val_bytes;
      dest.intent_bytes += src.intent_bytes;
      dest.live_count += src.live_count;
      dest.key_count += src.key_count;
      dest.val_count += src.val_count;
      dest.intent_count += src.intent_count;
      dest.intent_age += src.intent_age;
      dest.gc_bytes_age += src.gc_bytes_age;
      dest.sys_bytes += src.sys_bytes;
      dest.sys_count += src.sys_count;
      dest.last_update_nanos = Math.max(dest.last_update_nanos, src.last_update_nanos);
    }

    /*****************************
     * roachpb/metadata.proto
     ****************************/

    /**
     * Address is used to represent a network address.
     *
     * Source message = "Addr".
     */
    export interface Addr {
      network: string;
      address: string;
    }

    /**
     * StoreCapacity details the used and available capacity of a store.
     */
    export interface StoreCapacity {
      capacity: number;
      available: number;
      range_count: number;
    }

    /**
     * NodeDescriptor contains identifying characteristics of a node.
     */
    export interface NodeDescriptor {
      node_id: number;
      address: Addr;
      attrs: any;
    }

    /**
     * StoreDescriptor contains identifying characteristics of a store.
     */
    export interface StoreDescriptor {
      store_id: number;
      node: NodeDescriptor;
      attrs: any;
      capacity: StoreCapacity;
    }

    /*****************************
     * server/status/status.proto
     ****************************/

    /**
     * NodeStatus describes the high-level current status of a Node.
     */
    export interface NodeStatus {
      desc: NodeDescriptor;
      store_ids: number[];
      range_count: number;
      started_at: number;
      updated_at: number;
      stats: MVCCStats;
      leader_range_count: number;
      replicated_range_count: number;
      available_range_count: number;
    }

    /*****************************
     * storage/status.proto
     ****************************/

    /**
     * StoreStatus describes the high-level current status of a Store.
     */
    export interface StoreStatus {
      desc: StoreDescriptor;
      range_count: number;
      started_at: number;
      updated_at: number;
      stats: MVCCStats;
      leader_range_count: number;
      replicated_range_count: number;
      available_range_count: number;
    }

    /**
     * Status is the common interface shared by NodeStatus and StoreStatus.
     */
    export interface Status {
      range_count: number;
      started_at: number;
      updated_at: number;
      stats: MVCCStats;
      leader_range_count: number;
      replicated_range_count: number;
      available_range_count: number;
    }

    /**
     * AccumulateStauts accumulates values from a source status into
     * the values of a destination status value.
     */
    export function AccumulateStatus(dest: Status, src: Status): void {
      dest.range_count += src.range_count;
      dest.leader_range_count += src.leader_range_count;
      dest.replicated_range_count += src.replicated_range_count;
      dest.available_range_count += src.available_range_count;
      dest.updated_at = Math.max(dest.updated_at, src.updated_at);
      AccumulateMVCCStats(dest.stats, src.stats);
    }

    /*****************************
     * ts/timeseries.proto
     ****************************/

    /**
     * QueryAggregator is an enumeration of the available aggregator
     * functions for time series queries.
     *
     * Source message = "TimeSeriesQueryAggregator"
     */
    export enum QueryAggregator {
      AVG = 1,
      SUM = 2,
      MAX = 3,
      MIN = 4,
    }

    /**
     * QueryAggregator is an enumeration of the available derivative
     * functions for time series queries.
     *
     * Source message = "TimeSeriesQueryDerivative"
     */
    export enum QueryDerivative {
      NONE = 0,
      DERIVATIVE = 1,
      NON_NEGATIVE_DERIVATIVE = 2,
    }

    /**
     * Datapoint is a single datapoint in a query response.
     *
     * Source message = "TimeSeriesDatapoint"
     */
    export interface Datapoint {
      timestamp_nanos: number;
      value: number;
    }

    /**
     * QueryResult is a single query result.
     *
     * No direct source message. Historical relic.
     */
    export interface QueryResult {
      name: string;
      downsampler: QueryAggregator;
      source_aggregator: QueryAggregator;
      derivative: QueryDerivative;
      datapoints: Datapoint[];
    }

    /**
     * Result is a single query result.
     *
     * Source message = "TimeSeriesQueryResponse.Result"
     */
    export interface Result {
      datapoints: Datapoint[];
      query: QueryRequest;
    }

    /**
     * Results matches the successful output of the /ts/query
     * endpoint.
     *
     * Source message = "TimeSeriesQueryResponse"
     */
    export interface Results {
      results: Result[];
    }

    /**
     * QueryRequest is a single query request as expected by the server.
     *
     * Source message = "TimeSeriesQueryRequest.Query"
     */
    export interface QueryRequest {
      name: string;
      sources: string[];
      downsampler: QueryAggregator;
      source_aggregator: QueryAggregator;
      derivative: QueryDerivative;
    }

    /**
     * QueryRequestSet matches the expected input of the /ts/query endpoint.
     *
     * Source message = "TimeSeriesQueryRequest"
     */
    export interface QueryRequestSet {
      start_nanos: number;
      end_nanos: number;
      queries: QueryRequest[];
    }

    /*****************************
     * util/log/log.proto
     ****************************/

    /**
     * Arg represents an argument passed to a log entry.
     *
     * Source message = "LogEntry.Arg"
     */
    export interface Arg {
      type: string;
      str: string;
      json: string;
    }

    /**
     * LogEntry represents a cockroach structured log entry.
     *
     * Source message = "LogEntry"
     */
    export interface LogEntry {
      severity: number;
      time: number;
      file: string;
      line: number;
      format: string;
    }

    /*****************************
     * server/admin.proto
     ****************************/

    export interface Timestamp {
      sec?: number;
      nsec?: number;
    }

    export interface DatabaseList {
      databases: string[];
    }

    export interface Grant {
      database: string;
      privileges: string[];
      user: string;
    }

    export interface Database {
      grants: Grant[];
      table_names: string[];
    }

    export interface SQLColumn {
      name: string;
      type: string;
      nullable: boolean;
      default: string;
    }

    export interface SQLIndex {
      name: string;
      unique: boolean;
      seq: number;
      column: string;
      direction: string;
      storing: boolean;
    }

    export interface SQLTable {
      grants: Grant[];
      columns: SQLColumn[];
      indexes: SQLIndex[];
    }

    export interface User {
      username: string;
    }

    export interface Users {
      users: User[];
    }

    export interface UnparsedClusterEvent {
      timestamp: Timestamp;
      event_type: string;
      target_id: number;
      reporting_id: number;
      info: string;
    }

    export interface UnparsedClusterEvents {
      events: UnparsedClusterEvent[];
    }

    export interface EventInfo {
      DatabaseName?: string;
      TableName?: string;
      User?: string;
      Statement?: string;
      DroppedTables?: string[];
    }

    export interface SetUIDataRequest {
      key: string;
      value: string; // base64 encoded value
    }

    export interface GetUIDataRequest {
      key: string;
    }

    export interface GetUIDataResponse {
      value: string; // base64 encoded value
      last_updated: Timestamp;
    }
  }
}
