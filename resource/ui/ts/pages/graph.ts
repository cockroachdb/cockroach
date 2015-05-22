// source: controllers/monitor.ts
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
       * A small demonstration of a chart, which displays three graphs
       * displaying data for store 1 over the last ten minutes.
       */
      export module Page {
          export function controller() {}
          export function view() {
              var end = new Date();
              var start = new Date(end.getTime() - (10 * 60 * 1000));
              return m(".graphPage", [
                      m("H3", "Graph Demo"),
                      Components.Metrics.LineGraph.create(500, 350, 
                          new Models.Metrics.Query(start, end, "cr.store.livebytes.1")),
                      Components.Metrics.LineGraph.create(500, 350, 
                          new Models.Metrics.Query(start, end, "cr.store.keybytes.1")),
                      Components.Metrics.LineGraph.create(500, 350, 
                          new Models.Metrics.Query(start, end, "cr.store.livebytes.1", "cr.store.valbytes.1")),
              ]);
          }
      }
  }
}
