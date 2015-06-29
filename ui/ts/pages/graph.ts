// source: pages/graph.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="../util/querycache.ts" />
/// <reference path="../components/metrics.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  /**
   * Graph is a proof of concept for time series graphs.
   */
  export module Graph {
    /**
     * A small demonstration of a chart, which displays a two charts
     * displaying the same data set as retrieved by a QueryManager.
     */
    export module Page {
      import Metrics = Models.Metrics;

      class Controller implements _mithril.MithrilController {
        manager: Metrics.Executor;
        axis: Metrics.Axis;
        showRates: boolean;
        interval: number;

        timespan: Metrics.Time.TimeSpan = Metrics.Time.Recent(10 * 60 * 1000);

        toggleGraph: () => void = () => {
          this.showRates = !this.showRates;
          if (this.showRates) {
            this.axis.selectors([this.successRate, this.errorRate])
              .label("Count / 10sec");
          } else {
            this.axis.selectors([this.successCount, this.errorCount])
              .label("Count");
          }
        };

        // Define selectors.
        private successCount: Metrics.Select.Selector = Metrics.Select.Avg("cr.node.calls.success.1")
          .title("Successful calls");
        private errorCount: Metrics.Select.Selector = Metrics.Select.Avg("cr.node.calls.error.1")
          .title("Error calls");
        private successRate: Metrics.Select.Selector = Metrics.Select.AvgRate("cr.node.calls.success.1")
          .title("Successful call rate");
        private errorRate: Metrics.Select.Selector = Metrics.Select.AvgRate("cr.node.calls.error.1")
          .title("Error call rate");

        // Define query.
        private query: Metrics.Query = Metrics.NewQuery(
          this.successCount,
          this.errorCount,
          this.successRate,
          this.errorRate)
          .timespan(this.timespan);

        onunload(): void {
          clearInterval(this.interval);
        }

        constructor() {
          this.manager = new Metrics.Executor(this.query);
          this.axis = Metrics.NewAxis(this.successCount, this.errorCount)
            .label("Count");
          this.manager.refresh();
          this.interval = setInterval(() => this.manager.refresh(), 10000);
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let buttonText: string;
        if (ctrl.showRates) {
          buttonText = "Show Totals";
        } else {
          buttonText = "Show Rates";
        }
        return m(".graphPage", [
          m("H3", "Graph Demo"),
          Components.Metrics.LineGraph.create(ctrl.manager, ctrl.axis),
          Components.Metrics.LineGraph.create(ctrl.manager, ctrl.axis),
          m("",
            m("input[type=button]", {
              value: buttonText,
              onclick: ctrl.toggleGraph
            })),
        ]);
      }
    }
  }
}
