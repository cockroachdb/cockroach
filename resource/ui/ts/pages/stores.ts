// source: pages/stores.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/store_status.ts" />

// Author: Bram Gruneir (bram.gruneir@gmail.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  /**
   * Stores is the view for exploring the status of all Stores.
   */
  export module Stores {
    // TODO(bram): Lots of duplicate code between here and nodes.ts, refactor
    // it into one place.
    export interface RefreshFunction {
      f: () => _mithril.MithrilPromise<any>;
      o: any;
    }

    export interface QueryManagerSourceMap {
      [storeId: number]: { [source: string]: Models.Metrics.QueryManager }
    }

    export var storeStatuses: Models.StoreStatus.Stores = new Models.StoreStatus.Stores();
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

      private static _queryManagerBuilder(storeId: string, agg: Models.Metrics.QueryAggregator, source: string): Models.Metrics.QueryManager {
        var query = new Models.Metrics.RecentQuery(10 * 60 * 1000, agg, "cr.store." + source + "." + storeId);
        return new Models.Metrics.QueryManager(query);
      }

      private _addChart(agg: Models.Metrics.QueryAggregator, source: string): void {
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
          this._addChart(Models.Metrics.QueryAggregator.AVG, "keycount");
          this._addChart(Models.Metrics.QueryAggregator.AVG, "valcount");
          this._addChart(Models.Metrics.QueryAggregator.AVG, "livecount");
          this._addChart(Models.Metrics.QueryAggregator.AVG, "intentcount");
          this._addChart(Models.Metrics.QueryAggregator.AVG, "ranges");

          //TODO(Bram): Byte charts won't display properly.
          //this._addChart(Models.Metrics.QueryAggregator.AVG, "valbytes");
          //this._addChart(Models.Metrics.QueryAggregator.AVG, "livebytes");
          //this._addChart(Models.Metrics.QueryAggregator.AVG, "intentbytes");
          //this._addChart(Models.Metrics.QueryAggregator.AVG, "keybytes");
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
            Object.keys(storeStatuses.desc()).sort().map(function(storeId) {
              var desc = storeStatuses.desc()[storeId];
              return m("li", { key: desc.store_id },
                m("div", [
                  m.trust("&nbsp;&bull;&nbsp;"),
                  m("a[href=/stores/" + storeId + "]", { config: m.route }, "Store:" + storeId),
                  " on ",
                  m("a[href=/nodes/" + desc.node.node_id + "]", { config: m.route },"Node:" + desc.node.node_id),
                  " with Address:" + desc.node.address.network + "-" + desc.node.address.address
                ]));
            }),
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
