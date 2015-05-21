// source: controllers/monitor.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
    export module Metrics {
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
         * Query wraps the information needed to make a single time series query
         * to the server.
         */
        export class Query {
            private series:string[]
            constructor(public start:Date, public end:Date, ...series:string[]) {
                this.series = series
            }

            query():_mithril.MithrilPromise<QueryResultSet> {
                var url = "/ts/query";
                var data = {
                    start_nanos: this.start.getTime() * 1.0e6,
                    end_nanos: this.end.getTime() * 1.0e6,
                    queries: this.series.map((r) => {return {name: r};}),
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
