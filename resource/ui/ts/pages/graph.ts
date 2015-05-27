// source: pages/graph.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../models/timeseries.ts" />
/// <reference path="../components/metrics.ts" />

// Author: Bram Gruneir (bram.gruneir@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  /**
   * Graph is a proof of concept for time series graphs.
   */
  export module Graph {
      /**
       * A small demonstration of a chart, which displays a two charts
       * displaying the same data set as retrieved by a QueryManager.
       */
      export module Page {
          interface Controller extends _mithril.MithrilController {
              manager:Models.Metrics.QueryManager;
          }

          export function controller():Controller {
              var query = new Models.Metrics.RecentQuery(10 * 60 * 1000, "cr.node.calls.success.1");
              var manager = new Models.Metrics.QueryManager(query);
              manager.refresh();
             
              // Refresh interval on a regular basis.
              var interval = setInterval(() => manager.refresh(), 10000);
              return {
                  manager: manager, 
                  // Clear interval when page is unloaded.
                  onunload: () => clearInterval(interval),
              }
          }

          export function view(ctrl:Controller) {
              var windowSize = 10 * 60 * 1000;
              return m(".graphPage", [
                      m("H3", "Graph Demo"),
                      Components.Metrics.LineGraph.create(ctrl.manager),
                      Components.Metrics.LineGraph.create(ctrl.manager),
              ]);
          }
      }
  }
}
