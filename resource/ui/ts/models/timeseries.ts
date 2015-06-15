// source: models/timeseries.ts
// TODO(mrtracy): rename to metrics.ts.
/// <reference path="proto.ts" />
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
                request():Proto.QueryRequest;
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

                request = ():Proto.QueryRequest => {
                    return {
                        name:this.series_name,
                        aggregator:Proto.QueryAggregator.AVG,
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

                request = ():Proto.QueryRequest => {
                    return {
                        name:this.series_name,
                        aggregator:Proto.QueryAggregator.AVG_RATE,
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
        export class Query {
            constructor(private _selectors:select.Selector[]) {}

            /**
             * timespan gets or sets the TimeSpan over which data should be
             * queried. By default, the query will return the last ten minutes
             * of data.
             */
            timespan = Utils.chainProp(this, time.Recent(10 * 60 * 1000));

            /**
             * title gets or sets the title of this query, which can be applied
             * to visualizations of the data from this query.
             */
            title = Utils.chainProp(this, "Query Title");

            /**
             * execute dispatches a query to the server and returns a promise
             * for the results.
             */
            execute = ():promise<Proto.QueryResultSet> => {
                var s = this.timespan().timespan();
                var req:Proto.QueryRequestSet = {
                    start_nanos: Utils.Convert.MilliToNano(s[0]),
                    end_nanos: Utils.Convert.MilliToNano(s[1]),
                    queries:[],
                }
                for (var i = 0; i < this._selectors.length; i++) {
                    req.queries.push(this._selectors[i].request())
                }
                return Query.dispatch_query(req)
            }

            private static dispatch_query(q:Proto.QueryRequestSet):promise<Proto.QueryResultSet> {
                var url = "/ts/query";
                return m.request({url:url, method:"POST", extract:nonJsonErrors, data:q})
                    .then((d:Proto.QueryResultSet) => {
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
        export function NewQuery(...selectors:select.Selector[]):Query {
            return new Query(selectors);
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
