// source: components/metrics.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/d3/d3.d.ts" />
/// <reference path="../util/types.ts" />
/// <reference path="../util/querycache.ts" />
/// <reference path="../models/metrics.ts" />

/**
 * Components defines reusable components which may be used on multiple pages,
 * or multiple times on the same page.
 */
module Components {
  "use strict";
  /**
   * nv charts module does not currently have a typescript definition.
   */
  declare var nv: any;

  /**
   * Metrics contains components used to display metrics data.
   */
  export module Metrics {

    /**
     * LineGraph displays a line graph of the data returned from a time
     * series query.
     */
    export module LineGraph {
      import MithrilElementConfig = _mithril.MithrilElementConfig;
      /**
       * ViewModel is the model for a specific LineGraph - in addition to
       * the backing Query object, it also maintains other per-component
       * display options.
       */
      interface ViewModel {
        query: Models.Metrics.Executor;
        axis: Models.Metrics.Axis;
        key?: number;
      }

      /**
       * Controller contains the bulk of functionality needed to render a
       * LineGraph.
       */
      class Controller {
        static colors: d3.scale.Ordinal<Domain, string> = d3.scale.category10();

        // nvd3 chart.
        chart: any;

        constructor(public vm: ViewModel) {
          if (vm.axis.stacked()) {
            this.chart = nv.models.stackedAreaChart()
              .showControls(false);
          } else {
            this.chart = nv.models.lineChart();
          }
          this.chart
            .x((d: Models.Proto.Datapoint) => new Date(d.timestamp_nanos / 1.0e6))
            .y((d: Models.Proto.Datapoint) => d.value)
            // .interactive(true)
            .useInteractiveGuideline(true)
            .showLegend(true)
            .showYAxis(true)
            .showXAxis(true)
            .xScale(d3.time.scale())
            .margin({top: 20, right: 60, bottom: 20, left: 60});

          // Set xAxis ticks to properly format.
          this.chart.xAxis
            .tickFormat(function(t: string|Date): string { return typeof t === "string" ? t : d3.time.format("%H:%M:%S")(t); })
            .showMaxMin(false);
          this.chart.yAxis
            .axisLabel(vm.axis.label())
            .showMaxMin(false);

          if (vm.axis.format()) {
            this.chart.yAxis.tickFormat(vm.axis.format());
          }

          let range: number[] = vm.axis.range();

          if (range) {
            if (vm.axis.stacked() && range.length === 2) {
              this.chart.yDomain(range);
            } else if (!vm.axis.stacked()) {
              this.chart.forceY(range);
            }
          }

          // this.chart.stacked(vm.axis.stacked());
        }

        /**
         * drawGraph gets direct access to the svg element of the graph
         * after it is added to DOM. We use NVD3 to draw the data from
         * the query.
         */
        drawGraph: MithrilElementConfig = (element: Element, isInitialized: boolean, context: any) => {
          if (!isInitialized) {
            nv.addGraph(this.chart);
          }

          // TODO(mrtracy): Update if axis changes. NVD3 unfortunately
          // breaks mithril's assumption that everything is rendering
          // after every change, so we need to figure out the best way
          // to signal to components like this.
          let shouldRender: boolean = !context.epoch || context.epoch < this.vm.query.result.Epoch();

          if (shouldRender) {
            this.chart.showLegend(this.vm.axis.selectors().length > 1 && this.vm.axis.selectors().length < 4);
            let formattedData: any[] = [];
            // The result() property will be empty if an error
            // occurred. For now, we will just display the "No Data"
            // message until we decided on the proper way to display
            // error messages.
            let qresult: Models.Metrics.QueryInfoSet<Models.Proto.QueryResult> = this.vm.query.result();
            if (qresult) {
              // Iterate through each selector on the axis,
              // allowing each to select the necessary data from
              // the result.
              this.vm.axis.selectors().forEach((s: Models.Metrics.Select.Selector) => {
                let key: string = Models.Metrics.QueryInfoKey(s.request());
                let result: Models.Proto.QueryResult = qresult.get(key);
                if (result) {
                  formattedData.push({
                    values: result.datapoints,
                    key: s.title(),
                    color: Controller.colors(s.series()),
                    area: true,
                    fillOpacity: .1,
                  });
                }
              });
            }
            d3.select(element)
              .datum(formattedData)
              .transition().duration(500)
              .call(this.chart);
          }

          context.epoch = this.vm.query.result.Epoch();
        };

        /**
         * hasData returns true if graph data is available to render.
         */
        hasData(): boolean {
          return this.vm.query.result.Epoch() > 0;
        }
      }

      export function controller(model: ViewModel): Controller {
        return new Controller(model);
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        let g: _mithril.MithrilVirtualElement = null;
        if (ctrl.hasData()) {
          g = m(".linegraph", m("svg.graph", { config: ctrl.drawGraph }));
        } else {
          g = m("", "loading...");
        }
        return m(
          ".visualization-wrapper",
          [
            // TODO: pass in and display info icon tooltip
            m(".viz-top", m(".viz-info-icon", m(".icon-cockroach-17"))), // Icon Cockroach 17 is the info icon.
            g,
            m(".viz-bottom", m(".viz-title", ctrl.vm.axis.title())),
          ]
        );
      }

      /**
       * create instantiates a single instance of the LineGraph component
       * which displays data from the supplied Metrics.Query object.
       *
       * @param key The key param is used by mithril to track objects in lists which can be rearranged.
       */
      export function create(query: Models.Metrics.Executor, axis: Models.Metrics.Axis, key?: number): _mithril.MithrilComponent<Controller> {
        let vm: ViewModel = { query: query, axis: axis };
        if (key) {
          vm.key = key;
        }
        return m.component(LineGraph, vm);
      }
    }
  }
}
