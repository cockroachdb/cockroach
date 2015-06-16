// source: util/querycache.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)
//
module Utils {
    import promise = _mithril.MithrilPromise;
    import property = _mithril.MithrilProperty;

    /**
     * QueryCache supports caching the result of an arbitrary query so that the
     * result of that query can be shared by multiple components.
     *
     * The initial state of the QueryCache will have no data; the query function
     * will be called immediately when the QueryCache is constructed, but data
     * will not yet be available. When the query completes, the QueryCache will
     * contain either the result of the Query or an Error that resulted from the
     * Query.
     *
     * The QueryCache can be refreshed at any time, which will re-issue the same
     * query and replace the cached value. The original cache value will not be
     * replaced until the new query completes. Each time the query is refreshed,
     * the value of QueryCache.epoch() is increased.
     */
    export class QueryCache<T> {
        private _result:T = null;
        private _error:Error = null;
        private _epoch:number = 0;

        /**
         * Construct a new QueryCache which caches the ultimate results of the
         * given query function. It is expected that the query function returns
         * a promise for results.
         */
        constructor(private _query:() => promise<T>) {
            this.refresh()
        }

        /**
         * Refresh invokes the underlying query function, unless another
         * invocation is already in progress. The currently cached results (if
         * any) are not replaced until the query invocation completes.
         */
        refresh() {
            // Clear outstanding result if it has already returned.
            this.processOutstanding();
            if (!this._outstanding) {
                this._outstanding = {
                    result:this._query(),
                    error:m.prop(<Error> null),
                }
                this._outstanding.result.then(null, this._outstanding.error);
            }
        }

        /**
         * hasData returns true if at least one query has completed. This
         * indicates that either the result() or error() functions will return
         * non-null.
         */
        hasData():boolean {
            this.processOutstanding();
            return this._epoch > 0;
        }

        /**
         * result returns the result of most recent invocation of the underlying
         * query. If the most recent invocation returned an error, this method will
         * return null.
         */
        result():T {
            this.processOutstanding();
            return this._result;
        }

        /**
         * result returns any error that resulted from the most recent
         * invocation of the underlying query. If the most recent invocation did
         * not return an error, this method will return null.
         */
        error():Error {
            this.processOutstanding();
            return this._error;
        }

        /**
         * epoch returns the current epoch of the cached query. The epoch is
         * incremented each time the cached result is refreshed; the first
         * result has an epoch of 1.
         */
        epoch():number {
            this.processOutstanding();
            return this._epoch;
        }

        /**
         * This structure will be non-null when a query is in-flight, or has
         * completed but not been processed. When an in-flight query
         * completes, one and only one of these fields will contain a value.
         */
        private _outstanding:{
            result:promise<T>;
            error:property<Error>;
        }

        /**
         * Check for a completed outstanding query, replacing cached results
         * with the result.
         */
        private processOutstanding() {
            if (this._outstanding) {
                var completed =
                    (this._outstanding.error() != null || this._outstanding.result() != null);

                if (completed) {
                    this._result = this._outstanding.result();
                    this._error = this._outstanding.error();
                    this._outstanding = null;
                    this._epoch++;
                }
            }
        }
    }
}
