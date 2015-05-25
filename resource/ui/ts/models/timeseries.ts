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
         * Datapoint is a single datapoint in a query response. This needs to be
         * kept in sync with the TimeSeriesDatapoint protobuffer message on the
         * server. (/protos/timeseries.go).
         */
        export interface Datapoint {
            timestamp_nanos: number;
            value: number;
        }

        /**
         * QueryResult is a single query result. This needs to be kept in sync
         * with the TimeSeriesQueryResult.Query protobuffer message on the server.
         * (/protos/timeseries.go).
         */
        export interface QueryResult {
            name:string;
            datapoints: Datapoint[]
        }

        /**
         * QueryResultSet matches the successful output of the /ts/query
         * endpoint. This needs to be kept in sync with the
         * TimeSeriesQueryResult protobuffer message on the server.
         * (/protos/timeseries.go).
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
        function query(start:Date, end:Date, series:string[]):promise<QueryResultSet> {
            var url = "/ts/query";
            var data = {
                start_nanos: start.getTime() * 1.0e6,
                end_nanos: end.getTime() * 1.0e6,
                queries: series.map((r) => {return {name: r};}),
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
         * StaticQuery is a time series query with a constant start and end
         * time.
         */
        export class StaticQuery {
            private series:string[];
            private data:promise<QueryResultSet>;
            constructor(public start:Date, public end:Date, ...series:string[]) {
                this.series = series;
            }

            query():promise<QueryResultSet> {
                if (this.data != null) {
                    return this.data;
                }
                this.data = query(this.start, this.end, this.series);
                return this.data
            }
        }

        /** 
         * SlidingQuery is a query which monitors a duration of constant size
         * extending backwards from the current time. The query can be updated,
         * which will re-run the query starting from the current time.
         */
        export class SlidingQuery {
            private series:string[];
            constructor(public windowDuration:number, ...series:string[]) {
                this.series = series;
            }

            query():promise<QueryResultSet> {
                var endTime = new Date();
                var startTime = new Date(endTime.getTime() - this.windowDuration);
                return query(startTime, endTime, this.series)
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
