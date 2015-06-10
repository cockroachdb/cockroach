// source: pages/stores.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../models/timeseries.ts" />
/// <reference path="../components/metrics.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  /**
   * Stores is the view for exploring the status of all Stores.
   */
  export module Stores {
	import metrics = Models.Metrics;

    // TODO(bram): Lots of duplicate code between here and nodes.ts, refactor
    // it into one place.
    export interface RefreshFunction {
      f: () => _mithril.MithrilPromise<any>;
      o: any;
    }

    export interface QueryManagerSourceMap {
      [storeId: number]: { [source: string]: metrics.QueryManager }
    }

    export var storeStatuses = new Models.Status.Stores();
    export var queryManagers: QueryManagerSourceMap = {};

    export class Controller implements _mithril.MithrilController {
      private static _queryEveryMS = 10000;
      private _interval: number;
      private _storeId: string;
      private _refreshFunctions: RefreshFunction[]

      private _refresh(): void {
        for (var i = 0; i < this._refreshFunctions.length; i++) {
          this._refreshFunctions[i].f.call(this._refreshFunctions[i].o);
        }
      }

      private static _queryManagerBuilder(storeId: string, agg: Models.Proto.QueryAggregator, source: string): metrics.QueryManager {
		// TODO(mrtracy): This page has an impending, significant refactoring.
		// The query API was changed after this page was originally built, and
		// now uses composed functions to describe queries. Because this page
		// page never uses the rate aggregator, 
        var query = metrics.NewQuery(
			metrics.select.Avg("cr.store." + source + "." + storeId)
		).timespan(metrics.time.Recent(10 * 60 * 1000));
        return new metrics.QueryManager(query);
      }

      private _addChart(agg: Models.Proto.QueryAggregator, source: string): void {
        var name = agg + ":" + source;
        if (queryManagers[this._storeId][name] == null) {
          queryManagers[this._storeId][name] = Controller._queryManagerBuilder(this._storeId, agg, source);
        }
        this._refreshFunctions.push({ f: queryManagers[this._storeId][name].refresh, o: queryManagers[this._storeId][name] });
      }

      public constructor(storeId?:string) {
        this._refreshFunctions = [{
          f: storeStatuses.Query,
          o: storeStatuses
        }];

        if (storeId != null) {
          this._storeId = storeId;

          // Add a collection of charts.
          if (queryManagers[storeId] == null) {
            queryManagers[storeId] = {};
          }
          this._addChart(Models.Proto.QueryAggregator.AVG, "keycount");
          this._addChart(Models.Proto.QueryAggregator.AVG, "valcount");
          this._addChart(Models.Proto.QueryAggregator.AVG, "livecount");
          this._addChart(Models.Proto.QueryAggregator.AVG, "intentcount");
          this._addChart(Models.Proto.QueryAggregator.AVG, "ranges");
        } else {
          this._storeId = null;
        }

        this._refresh();
        this._interval = setInterval(() => this._refresh(), Controller._queryEveryMS);
      }

      public onunload() {
        clearInterval(this._interval);
      }
    }

    /**
     * StoresPage show a list of all active stores.
     */
    export module StoresPage {
      export function controller():Controller {
        return new Controller();
      }
      export function view(crtl:Controller) {
        return m("div", [
          m("h2", "Stores List"),
          m("ul", [
            storeStatuses.GetStoreIds().map(function(storeId) {
              var desc = storeStatuses.GetDesc(storeId);
              return m("li", { key: desc.store_id },
                m("div", [
                  m.trust("&nbsp;&bull;&nbsp;"),
                  m("a[href=/stores/" + storeId + "]", { config: m.route }, "Store:" + storeId),
                  " on ",
                  m("a[href=/nodes/" + desc.node.node_id + "]", { config: m.route },"Node:" + desc.node.node_id),
                  " with Address:" + desc.node.address.network + "-" + desc.node.address.address
                ]));
            }),
            storeStatuses.AllDetails()
          ]),
        ]);
      }
    }

    /**
     * StorePage show the details of a single node.
     */
    export module StorePage {
      export function controller():Controller {
        var storeId = m.route.param("store_id");
        return new Stores.Controller(storeId);
      }
      export function view(crtl:Controller) {
        var storeId = m.route.param("store_id");
        return m("div", [
          m("h2", "Store Status"),
          m("div", [
            m("h3", "Store: " + storeId),
            storeStatuses.Details(storeId)
          ]),
          m("table", [
            m("tr", [
              m("td", [
                m("h4", "Key Count"),
                Components.Metrics.LineGraph.create(Stores.queryManagers[storeId]["1:keycount"])
              ]),
              m("td", [
                m("h4", "Value Count"),
                Components.Metrics.LineGraph.create(Stores.queryManagers[storeId]["1:valcount"])
              ])
            ]),
            m("tr", [
              m("td", [
                m("h4", "Live Count"),
                Components.Metrics.LineGraph.create(Stores.queryManagers[storeId]["1:livecount"])
              ]),
              m("td", [
                m("h4", "Intent Count"),
                Components.Metrics.LineGraph.create(Stores.queryManagers[storeId]["1:intentcount"])
              ])
            ]),
            m("tr", [
              m("td", [
                m("h4", "Range Count"),
                Components.Metrics.LineGraph.create(Stores.queryManagers[storeId]["1:ranges"])
              ]),
              m("td")
            ])
          ])
        ]);
      }
    }
  }
}
