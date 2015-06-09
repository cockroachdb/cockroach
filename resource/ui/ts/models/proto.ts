// source: models/proto.ts
// Author: Matt Tracy (matt@cockroachlabs.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

module Models {
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
         * /proto/data.proto
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

        /*****************************
         * /proto/config.proto
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
            Capacity: number;
            Available: number;
            RangeCount: number;
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
            Capacity: StoreCapacity;
        }

        /*****************************
         * /proto/status.proto
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

        /*****************************
         * /proto/timeseries.proto
         ****************************/

        /** 
         * QueryAggregator is an enumeration of the available aggregator
         * functions for time series queries. 
         *
         * Source message = "TimeSeriesQueryAggregator"
         */
        export enum QueryAggregator {
            AVG = 1,
            AVG_RATE = 2,
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
         * Source message = "TimeSeriesQueryResponse.Result"
         */
        export interface QueryResult {
            name:string;
            datapoints: Datapoint[]
        }

        /**
         * QueryResultSet matches the successful output of the /ts/query
         * endpoint. 
         *
         * Source message = "TimeSeriesQueryResponse"
         */
        export interface QueryResultSet {
            results: QueryResult[];
        }


        /**
         * QueryRequest is a single query request as expected by the server.
         * 
         * Source message = "TimeSeriesQueryRequest.Query"
         */
        export interface QueryRequest {
            name:string;
            aggregator:QueryAggregator;
        }

        /**
         * QueryRequestSet matches the expected input of the /ts/query endpoint.
         *
         * Source message = "TimeSeriesQueryRequest"
         */
        export interface QueryRequestSet {
            start_nanos:number;
            end_nanos:number;
            queries:QueryRequest[];
        }
    }
}
