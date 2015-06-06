// source: pages/graph.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../models/timeseries.ts" />
/// <reference path="../components/metrics.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)
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
          import metrics = Models.Metrics;
          class Controller implements _mithril.MithrilController {
              manager:Models.Metrics.QueryManager;
              showRates:boolean;
              interval:number;

              timespan = metrics.time.Recent(10 * 60 * 1000);
              sumquery = metrics.NewQuery(
					  metrics.select.Avg("cr.node.calls.success.1").title("Successful calls"),
					  metrics.select.Avg("cr.node.calls.error.1").title("Error calls")
					)
				  .timespan(this.timespan);
              ratequery = metrics.NewQuery(metrics.select.AvgRate("cr.node.calls.success.1"))
				  .timespan(this.timespan);
						  
              constructor(){
                  this.manager = new Models.Metrics.QueryManager(this.sumquery);
                  this.manager.refresh();
                  this.interval = setInterval(() => this.manager.refresh(), 10000);
              }

              onunload() {
                  clearInterval(this.interval);
              }

              toggleGraph = () => {
                  this.showRates = !this.showRates;
                  if (this.showRates) {
                      this.manager.setQuery(this.ratequery);
                  } else {
                      this.manager.setQuery(this.sumquery);
                  }
                  this.manager.refresh();
              }
          }

          export function controller():Controller {
              return new Controller();
          }

          export function view(ctrl:Controller) {
              var buttonText:string;
              if (ctrl.showRates) {
                  buttonText = "Show Totals";
              } else {
                  buttonText = "Show Rates";
              }
              return m(".graphPage", [
                      m("H3", "Graph Demo"),
                      Components.Metrics.LineGraph.create(ctrl.manager),
                      Components.Metrics.LineGraph.create(ctrl.manager),
                      m("",
                          m("input[type=button]", {
                              value: buttonText,
                              onclick: ctrl.toggleGraph,
                          })),
              ]);
          }
      }
  }
}
