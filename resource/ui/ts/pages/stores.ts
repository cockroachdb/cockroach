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
    export var storeStatuses: Models.StoreStatus.Stores = new Models.StoreStatus.Stores();
    export class Controller implements _mithril.MithrilController {
      private static _queryEveryMS = 10000;
      private _interval: number;

      public constructor() {
        storeStatuses.Query();
        this._interval = setInterval(() => storeStatuses.Query(), Controller._queryEveryMS);
      }

      public onunload() {
        clearInterval(this._interval);
      }
    }

    /**
     * StoresPage show a list of all active stores.
     */
    export module StoresPage {
      export function controller() {
        return new Controller();
      }
      export function view() {
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
      export function controller() {
        return new Controller();
      }
      export function view() {
        var storeId = m.route.param("store_id");
        return m("div", [
          m("h2", "Store Status"),
          m("div", [
            m("h3", "Store: " + storeId),
            storeStatuses.Details(storeId)
          ])
        ]);
      }
    }
  }
}
