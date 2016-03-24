// source: components/metrics.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
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
      import Datapoint = Models.Proto.Datapoint;
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

      // Chart margins to match design.
      const CHART_MARGINS: nv.Margin = {top: 20, right: 60, bottom: 20, left: 60};
      // Maximum number of series we will show in the legend. If there are more we hide the legend.
      const MAX_LEGEND_SERIES: number = 3;

      /**
       * Controller contains the bulk of functionality needed to render a
       * LineGraph.
       */
      class Controller {
        static colors: d3.scale.Ordinal<Domain, string> = d3.scale.category10();

        // nvd3 chart.
        chart: nv.LineChart;

        constructor(public vm: ViewModel) {
          if (vm.axis.stacked()) {
            this.chart = nv.models.stackedAreaChart()
              .showControls(false);
          } else {
            this.chart = nv.models.lineChart();
          }

          this.chart
            .x((d: Models.Proto.Datapoint) => new Date(Utils.Convert.NanoToMilli(d && d.timestamp_nanos)))
            .y((d: Models.Proto.Datapoint) => d && d.value)
            // .interactive(true)
            .useInteractiveGuideline(true)
            .showLegend(true)
            .showYAxis(true)
            .showXAxis(vm.axis.xAxis())
            .xScale(d3.time.scale())
            // chart margins are set to
            .margin(CHART_MARGINS);

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

          // nvd3 doesn't support the same options for stacked area charts as for line charts. Generally we want
          // .forceY because it will expand the chart if the values exceed the values in the forceY array, but for stacked
          // area chart we can only support a strict [min, max] two element array.
          if (range) {
            // we sanity check that the range is the correct format (array of length 2) before applying it as the yDomain
            if (vm.axis.stacked() && range.length === 2) {
              this.chart.yDomain(range);
            } else if (!vm.axis.stacked()) {
              this.chart.forceY(range);
            } else {
              throw new Error("Unexpected range: " + range + ". " +
                "For a stacked area chart, the range must be an array of length 2.");
            }
          }
        }

        /**
         * drawGraph gets direct access to the svg element of the graph
         * after it is added to DOM. We use NVD3 to draw the data from
         * the query.
         */
        drawGraph: MithrilElementConfig = (element: Element, isInitialized: boolean, context: any) => {
          // TODO(mrtracy): Update if axis changes. NVD3 unfortunately
          // breaks mithril's assumption that everything is rendering
          // after every change, so we need to figure out the best way
          // to signal to components like this.
          let shouldRender: boolean = !context.epoch || context.epoch < this.vm.query.result.Epoch();

          if (shouldRender) {
            this.chart.showLegend(
              _.isBoolean(this.vm.axis.legend()) ? this.vm.axis.legend() :
              this.vm.axis.selectors().length > 1 && this.vm.axis.selectors().length <= MAX_LEGEND_SERIES);
            let formattedData: any[] = [];
            // The result() property will be empty if an error
            // occurred. For now, we will just display the "No Data"
            // message until we decided on the proper way to display
            // error messages.
            let qresult: Models.Metrics.QueryInfoSet<Models.Proto.QueryResult> = this.vm.query.lastResult();
            if (qresult) {
              // Iterate through each selector on the axis,
              // allowing each to select the necessary data from
              // the result.

              // AxisDomain is a helper class used for storing the min/max values to store on an axis
              class AxisDomain {
                min: number;
                max: number;
                constructor(min: number = Infinity, max: number = -Infinity) {
                  this.min = _.isNumber(min) ? min : this.min;
                  this.max = _.isNumber(max) ? max : this.max;
                }

                domain(): [number, number] {
                  return [this.min, this.max];
                }

                ticks(transform: (n: number) => any = _.identity): number[] {
                  return _.map(_.uniq([this.min, (this.min + this.max) / 2, this.max]), transform);
                }
              }

              let yAxisDomain: AxisDomain = new AxisDomain();
              let xAxisDomain: AxisDomain = new AxisDomain();

              let computeFullAxisDomain = (domain: AxisDomain, values: number[]): AxisDomain => {
                return new AxisDomain(
                  Math.min(domain.min, ...values),
                  Math.max(domain.max, ...values)
                );
              };

              this.vm.axis.selectors().forEach((s: Models.Metrics.Select.Selector) => {
                let key: string = Models.Metrics.QueryInfoKey(s.request());
                let result: Models.Proto.QueryResult = qresult.get(key);
                if (result) {
                  yAxisDomain = computeFullAxisDomain(yAxisDomain, _.map<Datapoint, number>(result.datapoints, "value"));
                  xAxisDomain = computeFullAxisDomain(xAxisDomain, _.map<Datapoint, number>(result.datapoints, "timestamp_nanos"));

                  formattedData.push({
                    values: result.datapoints,
                    key: s.title(),
                    color: Controller.colors(s.series()),
                    area: true,
                    fillOpacity: .1,
                  });
                }
              });

              // compute final y axis display range using yDomain and ylow/yhigh values on the axis
              yAxisDomain = new AxisDomain(
                _.isNumber(this.vm.axis.yLow()) ? Math.min(yAxisDomain.min, this.vm.axis.yLow()) : yAxisDomain.min,
                _.isNumber(this.vm.axis.yHigh()) ? Math.max(yAxisDomain.max, this.vm.axis.yHigh()) : yAxisDomain.max
              );
              this.chart.yDomain(yAxisDomain.domain());

              // always set the tick values to the lowest axis value, the highest axis value, and one value in between
              this.chart.yAxis.tickValues(yAxisDomain.ticks());
              this.chart.xAxis.tickValues(xAxisDomain.ticks((n: number): Date => new Date(Utils.Convert.NanoToMilli(n))));
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
        let icon: _mithril.MithrilVirtualElement = m(".icon-info");
        let warningClass = "";
        if (ctrl.hasData()) {
          g = m(".linegraph", m("svg.graph", { config: ctrl.drawGraph }));
          if (ctrl.vm.query.error() != null) {
            icon = m(".icon-warning", {
              title: ctrl.vm.query.error(),
            });
            warningClass = " .viz-faded";
          }
        } else {
          g = m("", "loading...");
        }
        return m(
          ".visualization-wrapper" + warningClass,
          [
            // TODO: pass in and display info icon tooltip
            m(".viz-top", m(".viz-info-icon", icon)),
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
