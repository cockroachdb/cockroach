// source: models/timeseries.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
    export module Metrics {
        import promise = _mithril.MithrilPromise;

        /** 
         * QueryAggregator is an enumeration of the available aggregator
         * functions for time series queries. This needs to be kept in sync with
         * the TimeSeriesQueryAggregator enumeration on the server.
         * (/protos/timeseries.proto)
         */
        export enum QueryAggregator {
            AVG = 1,
            AVG_RATE = 2,
        }

        /**
         * Datapoint is a single datapoint in a query response. This needs to be
         * kept in sync with the TimeSeriesDatapoint protobuffer message on the
         * server. (/protos/timeseries.proto).
         */
        export interface Datapoint {
            timestamp_nanos: number;
            value: number;
        }

        /**
         * QueryResult is a single query result. This needs to be kept in sync
         * with the TimeSeriesQueryResult.Query protobuffer message on the server.
         * (/protos/timeseries.proto).
         */
        export interface QueryResult {
            name:string;
            datapoints: Datapoint[]
        }

        /**
         * QueryResultSet matches the successful output of the /ts/query
         * endpoint. This needs to be kept in sync with the
         * TimeSeriesQueryResult protobuffer message on the server.
         * (/protos/timeseries.proto).
         */
        export interface QueryResultSet {
            results: QueryResult[];
        }

        /**
         * Query is a common interface implemented by the query types in * this module.
         */
        export interface Query {
            query:()=>promise<QueryResultSet>;
        }

        /**
         * Query dispatches a single time series query to the server.
         */
        function query(start:Date, end:Date, agg:QueryAggregator, series:string[]):promise<QueryResultSet> {
            var url = "/ts/query";
            var data = {
                start_nanos: start.getTime() * 1.0e6,
                end_nanos: end.getTime() * 1.0e6,
                queries: series.map((r) => {return {
                    name: r,
                    aggregator: agg,
                };}),
            }
        
            return m.request({url:url, method:"POST", extract:nonJsonErrors, data:data})
                .then((d:QueryResultSet) => {
                    // Populate missing collection fields with empty arrays.
                    if (!d.results) {
                        d.results = [];
                    }
                    d.results.forEach((r) => {
                        if (!r.datapoints) {
                            r.datapoints = []
                        }
                    });
                    return d;
                });
        }

        /** 
         * RecentQuery is a query which monitors a duration of constant size
         * extending backwards from the current time. When refreshed, the query
         * will be re-issued to extend to the current time.
         */
        export class RecentQuery {
            private _series:string[];
            constructor(public windowDuration:number, private _agg:QueryAggregator, ...series:string[]) {
                this._series = series;
            }

            query():promise<QueryResultSet> {
                var endTime = new Date();
                var startTime = new Date(endTime.getTime() - this.windowDuration);
                return query(startTime, endTime, this._agg, this._series);
            }
        }

        /**
         * QueryManager supports the sharing of query result data between
         * multiple components.
         */
        export class QueryManager {
            private _result:QueryResultSet = null;
            private _error:Error = null;
            private _resultEpoch:number = 0;

            // This structure will be non-null when a query is in-flight, or has
            // completed but not been processed. When an in-flight query
            // completes, only one of these fields will contain a value.
            private _outstanding:{
                result:promise<QueryResultSet>;
                error:_mithril.MithrilProperty<Error>;
            } = null

            /**
             * Construct a new QueryManager which obtains results from the
             * supplied query.
             */
            constructor(private _query:Query) {}

            private processOutstanding(){
                if (this._outstanding) {
                    var completed = 
                        (this._outstanding.error() != null || this._outstanding.result() != null);

                    if (completed) {
                        this._result = this._outstanding.result();
                        this._error = this._outstanding.error();
                        this._outstanding = null
                        this._resultEpoch++;
                    }
                }
            }

            /**
             * setQuery changes the query underlying this manager.
             */
            setQuery(q:Query){
                this._query = q;
            }

            /**
             * result returns the most recent result of the query, if any is
             * present.
             */
            result():QueryResultSet {
                this.processOutstanding();
                return this._result;
            }

            /**
             * epoch returns the epoch of the current query result; this is a
             * monotonically increasing counter which is incremented with each
             * query (whether successful or not). The first query result has an
             * epoch of 1.
             */
            epoch():number {
                this.processOutstanding();
                return this._resultEpoch;
            }

            /**
             * error returns the error resulting from the most recent call to
             * refresh(), if any occured.
             */
            error():Error {
                this.processOutstanding();
                return this._error;
            }

            refresh():promise<QueryResultSet> {
                // Clear outstanding request if it has already returned.
                this.result();
                if (!this._outstanding) {
                    this._outstanding = {
                        result:this._query.query(),
                        error:m.prop(<Error> null), 
                    }
                    this._outstanding.result.then(null, this._outstanding.error);
                }
                return this._outstanding.result;
            }
        }

        /**
         * nonJsonErrors ensures that error messages returned from the server
         * are parseable as JSON strings.
         */
        function nonJsonErrors(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions):string {
            return xhr.status > 200 ? JSON.stringify(xhr.responseText) : xhr.responseText;
        }
    }
}
