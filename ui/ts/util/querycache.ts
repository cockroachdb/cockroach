// source: util/querycache.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../util/property.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)
//
module Utils {
  "use strict";

  import promise = _mithril.MithrilPromise;

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

    private _result: Utils.Property<T> = Utils.Prop(<T> null);
    private _error: Utils.Property<Error> = Utils.Prop(<Error> null);
    private _inFlight: boolean = false;

    /**
     * Construct a new QueryCache which caches the ultimate results of the
     * given query function. It is expected that the query function returns
     * a promise for results. If dontRefresh is true, then the query cache will
     * not refresh until refresh is called.
     */
    constructor(private _query: () => promise<T>, dontRefresh?: boolean ) {
      this.result = this._result;
      this.error = this._error;
      if (!dontRefresh) {
        this.refresh();
      }
    }

    /**
     * Refresh invokes the underlying query function, unless another
     * invocation is already in progress. The currently cached results (if
     * any) are not replaced until the query invocation completes.
     */
    refresh(): void {
      if (this._inFlight) {
        return;
      }
      this._inFlight = true;

      this._query().then(
        (obj: T): T => {
          this._error(null);
          this._inFlight = false;
          return this._result(obj);
        },
        (err: Error): Error => {
          this._result(null);
          this._inFlight = false;
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
