// source: models/timeseries.ts
// TODO(mrtracy): rename to metrics.ts.
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../util/chainprop.ts" />
/// <reference path="../util/convert.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
    /**
     * Metrics package represents the internal performance metrics collected by
     * cockroach.
     */
    export module Metrics {
        import promise = _mithril.MithrilPromise;

        // TODO(mrtracy): Extract all of the protobuffer-based interfaces into a
        // 'proto' package.

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
         * with the TimeSeriesQueryResponse.Result protobuffer message on the server.
         * (/protos/timeseries.proto).
         */
        export interface QueryResult {
            name:string;
            datapoints: Datapoint[]
        }

        /**
         * QueryResultSet matches the successful output of the /ts/query
         * endpoint. This needs to be kept in sync with the
         * TimeSeriesQueryResponse protobuffer message on the server.
         * (/protos/timeseries.proto).
         */
        export interface QueryResultSet {
            results: QueryResult[];
        }


        /**
         * QueryRequest is a single query request as expected by the server.
         * This needs to be kept in sync with the TimeSeriesQueryRequest.Query
         * protobuffer message on the server.
         * (/protos/timeseries.proto).
         */
        export interface QueryRequest {
            name:string;
            aggregator:QueryAggregator;
        }

        /**
         * QueryRequestSet matches the expected input of the /ts/query endpoint.
         * This needs to be kept in sync with the TimeSeriesQueryRequest
         * protobuffer message on the server.
         * (/protos/timeseries.proto).
         */
        export interface QueryRequestSet {
            start_nanos:number;
            end_nanos:number;
            queries:QueryRequest[];
        }


        /**
         * select contains time series selectors for use in metrics queries. 
         * Each selector defines a dataset on the server which should be
         * queried, along with additional information about how to process the
         * data (e.g. aggregation functions, transformations) and displayed (e.g.
         * a friendly title for graph legends).
         */
        export module select {
            /**
             * Selector is the common interface implemented by all Selector
             * types. The is a read-only interface used by other components to
             * extract relevant information from the selector.
             */
            export interface Selector {
                /**
                 * request returns a QueryRequest object based on this selector.
                 */
                request():QueryRequest;
                /**
                 * title returns a display-friendly title for this series.
                 */
                title():string;
            }

            /**
             * AvgSelector selects the average value of the supplied time series.
             */
            class AvgSelector implements Selector {
                constructor(private series_name:string) {}

                title = Utils.chainProp(this, this.series_name);

                request = ():QueryRequest => {
                    return {
                        name:this.series_name,
                        aggregator:QueryAggregator.AVG,
                    }
                }
            }

            /**
             * AvgRateSelector selects the rate of change of the average value
             * of the supplied time series.
             */
            class AvgRateSelector implements Selector {
                constructor(private series_name:string) {}

                title = Utils.chainProp(this, this.series_name);

                request = ():QueryRequest => {
                    return {
                        name:this.series_name,
                        aggregator:QueryAggregator.AVG_RATE,
                    }
                }
            }

            /**
             * Avg instantiates a new AvgSelector for the supplied time series.
             */
            export function Avg(series:string):AvgSelector {
                return new AvgSelector(series);
            }

            /**
             * AvgRate instantiates a new AvgRateSelector for the supplied time
             * series.
             */
            export function AvgRate(series:string):AvgRateSelector {
                return new AvgRateSelector(series);
            }
        }

        /**
         * time contains available time span specifiers for metrics queries.
         */
        export module time {
            /**
             * TimeSpan is the interface implemeted by time span specifiers.
             */
            export interface TimeSpan {
                /**
                 * timespan returns a two-value number array which defines the
                 * time range of a query. The first value is a timestamp for the
                 * start of the range, the second value a timestamp for the end
                 * of the range.
                 */
                timespan():number[];
            }


            /**
             * Recent selects a duration of constant size extending backwards
             * from the current time. The current time is recomputed each time
             * Recent's timespan() method is called.
             */
            export function Recent(duration:number):TimeSpan {
                return {
                    timespan: function():number[] {
                        var endTime = new Date();
                        var startTime = new Date(endTime.getTime() - duration);
                        return [startTime.getTime(), endTime.getTime()]
                    }
                }
            }
        }

        /**
         * Query describes a single, repeatable query for time series data. Each
         * query contains one or more time series selectors, and a time span
         * over which to query those selectors.
         */
        class Query {
            constructor(private _selectors:select.Selector[]) {}

            /**
             * timespan gets or sets the TimeSpan over which data should be
             * queried. By default, the query will return the last ten minutes
             * of data.
             */
            timespan = Utils.chainProp(this, time.Recent(10 * 60 * 1000));

            /**
             * execute dispatches a query to the server and returns a promise
             * for the results.
             */
            execute():promise<QueryResultSet> {
                var s = this.timespan().timespan();
                var req:QueryRequestSet = {
                    start_nanos: Utils.milliToNanos(s[0]),
                    end_nanos: Utils.milliToNanos(s[1]),
                    queries:[],
                }
                for (var i = 0; i < this._selectors.length; i++) {
                    req.queries.push(this._selectors[i].request())
                }
                return Query.dispatch_query(req)
            }

            private static dispatch_query(q:QueryRequestSet):promise<QueryResultSet> {
                var url = "/ts/query";
                return m.request({url:url, method:"POST", extract:nonJsonErrors, data:q})
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
         * NewQuery constructs a new query object which queries the supplied
         * selectors. Additional properties of the query can be configured by
         * calling setter methods on the returned Query.
         */
        export function NewQuery(...selectors:select.Selector[]) {
            return new Query(selectors);
        }

        /**
         * QueryManager supports the sharing of query result data between
         * multiple components. This is useful when multiple components need to
         * synchronize on the same result set.
         */
        export class QueryManager {
            private _result:QueryResultSet = null;
            private _error:Error = null;
            private _resultEpoch:number = 0;

            // This structure will be non-null when a query is in-flight, or has
            // completed but not been processed. When an in-flight query
            // completes, only one of its fields will contain a value.
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
                        result:this._query.execute(),
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
