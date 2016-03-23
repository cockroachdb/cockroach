// source: util/querycache.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../util/property.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)
//
module Utils {
  "use strict";

  import promise = _mithril.MithrilPromise;
  import MithrilPromise = _mithril.MithrilPromise;

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
    /**
     * result returns the result of most recent invocation of the underlying
     * query. If the most recent invocation returned an error, this method will
     * return null.
     */
    result: Utils.ReadOnlyProperty<T>;

    /**
     * error returns any error that resulted from the most recent
     * invocation of the underlying query. If the most recent invocation did
     * not return an error, this method will return null.
     */
    error: Utils.ReadOnlyProperty<Error>;

    /**
     * lastResult returns the result of most recent invocation of the underlying
     * query which succeeded.
     */
    lastResult: Utils.ReadOnlyProperty<T>;

    private _result: Utils.Property<T> = Utils.Prop(<T> null);
    private _lastResult: Utils.Property<T> = Utils.Prop(<T> null);
    private _error: Utils.Property<Error> = Utils.Prop(<Error> null);
    private _inFlight: MithrilPromise<any> = null; // tracks the current promise

    /**
     * Construct a new QueryCache which caches the ultimate results of the
     * given query function. It is expected that the query function returns
     * a promise for results. If dontRefresh is true, then the query cache will
     * not refresh until refresh is called.
     */
    constructor(private _query: () => promise<T>, dontRefresh?: boolean ) {
      this.result = this._result;
      this.error = this._error;
      this.lastResult = this._lastResult;
      if (!dontRefresh) {
        this.refresh();
      }
    }

    /**
     * Refresh invokes the underlying query function, unless another
     * invocation is already in progress. The currently cached results (if
     * any) are not replaced until the query invocation completes.
     */
    refresh(): MithrilPromise<any> {
      if (this._inFlight) {
        return this._inFlight;
      }
      this._inFlight = this._query().then(
        (obj: T): T => {
          this._error(null);
          this._inFlight = null;
          this._lastResult(obj);
          return this._result(obj);
        },
        (err: Error): Error => {
          // Null error occurs when xhr status is 0 - this can result from a
          // timeout, a bad connection, who knows: it's difficult to extract
          // information from the failed request, apparently. As a stop gap, we
          // are going to fill in a generic "Connection to server failed."
          // message here, since that seems to be a general description of this
          // class of error. Development tools (such as chrome) can get more
          // information inside of the network monitor, but that does not appear
          // to be available to us inside of the code.
          if (err === null) {
            err = new Error("Connection to server failed.");
          }
          this._result(null);
          this._inFlight = null;
          return this._error(err);
        });
    }

    /**
     * hasData returns true if at least one query has completed. This
     * indicates that either the result() or error() functions will return
     * non-null.
     */
    hasData(): boolean {
      return this.result.Epoch() > 0 || this.error.Epoch() > 0;
    }
  }
}
