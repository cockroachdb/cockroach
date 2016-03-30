// source: pages/cluster.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts"/>
/// <reference path="../models/status.ts" />
/// <reference path="../models/events.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../components/topbar.ts" />
/// <reference path="../components/events.ts" />
/// <reference path="../util/format.ts" />
/// <reference path="../components/visualizations/visualizations.ts" />
/// <reference path="../components/visualizations/number.ts" />

// Author: Max Lang (max@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  import MithrilElement = _mithril.MithrilVirtualElement;
  import NavigationBar = Components.NavigationBar;

  /**
   * Cluster is the view for exploring the overall status of the cluster.
   */
  export module Cluster {
    import Metrics = Models.Metrics;
    import NodeStatus = Models.Proto.NodeStatus;
    import Selector = Models.Metrics.Select.Selector;
    import MithrilComponent = _mithril.MithrilComponent;
    import MetricNames = Models.Proto.MetricConstants;

    let nodeStatuses: Models.Status.Nodes = Models.Status.nodeStatusSingleton;

    function _nodeMetric(metric: string): string {
      return "cr.node." + metric;
    }

    // function _storeMetric(metric: string): string {
    //   return "cr.store." + metric;
    // }

    function _sysMetric(metric: string): string {
      return "cr.node.sys." + metric;
    }

    /**
     * Cluster Page
     */
    export module Page {
      class Controller {
        private static _queryEveryMS: number = 10000;

        private static defaultTargets: NavigationBar.Target[] = [
          {
            view: "Overview",
            route: "",
          },
          {
            view: "Events",
            route: "events",
          },
        ];

        exec: Metrics.Executor;
        axes: (any)[] = [];
        axesSmall: (any)[] = [];
        private _interval: number;
        private _query: Metrics.Query;

        private _quantiles: string[] = [
          "-max",
          "-p99",
          "-p75",
          "-p50",
        ];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
            return ((m.route.param("detail") || "") === t.route);
        };

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();

          this.axesSmall.push({
            titleFn: (allStats: Models.Proto.NodeStatus[]): string => {
              if (allStats && allStats.length === 1) {
                return "Node";
              }
              return "Nodes";
            },
            visualizationArguments: {
              format: "s",
              dataFn: function (allStats: Models.Proto.NodeStatus[]): { value: number; } { return {value: allStats && allStats.length || 0 }; },
            },
          });

          this.axesSmall.push({
            title: "Capacity Used",
            visualizationArguments: {
              format: "0.1%",
              dataFn: function (allStats: Models.Proto.NodeStatus[], totalStats: Models.Proto.StatusMetrics): { value: number; } {
                let capacity: number = totalStats[MetricNames.capacity];
                if (capacity === 0) {
                  // This usually happens, because the overall capacity is not
                  // yet known.
                  return {value: 0.0};
                }
                return {value: totalStats[MetricNames.liveBytes] /  capacity};
              },
              zoom: "50%",
            },
          });

          let latencySelectors: Selector[] = _.map(
            this._quantiles,
            (q: string): Selector => {
              return Metrics.Select.Avg(_nodeMetric("exec.latency-1m" + q))
                .title("Latency" + q);
            });
          let fmt: (v: number) => string = d3.format(".1f");
          this._addChartSmall(Metrics.NewAxis.apply(this, latencySelectors)
            .format((v: number): string => fmt(Utils.Convert.NanoToMilli(v)))
            .title("Query Time")
            .label("Milliseconds")
          );

          // TODO: should we use load instead of CPU?
          // TODO: range should take into account # of cpus
          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .title("CPU User %"),
              Metrics.Select.Avg(_sysMetric("cpu.sys.percent"))
                .title("CPU Sys %")
            ).format(d3.format(".2%")).title("CPU Usage").stacked(true)
          );

          // TODO: get total/average memory from all machines
          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("allocbytes"))
                .title("Memory")
            ).format(Utils.Format.Bytes).title("Memory Usage")
          );

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.conns"))
                .title("Connections")
              ).format(d3.format(".1")).title("SQL Connections")
          );

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.bytesin"))
                .nonNegativeRate()
                .title("Bytes In"),
              Metrics.Select.Avg(_nodeMetric("sql.bytesout"))
                .nonNegativeRate()
                .title("Bytes Out")
            ).format(Utils.Format.Bytes).title("SQL Traffic")
          );

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .nonNegativeRate()
                .title("Selects")
            ).format(d3.format(".1")).title("Reads")
          );

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.insert.count"))
                .nonNegativeRate()
                .title("Insert"),
              Metrics.Select.Avg(_nodeMetric("sql.update.count"))
                .nonNegativeRate()
                .title("Update"),
              Metrics.Select.Avg(_nodeMetric("sql.delete.count"))
                .nonNegativeRate()
                .title("Delete")
            ).format(d3.format(".1")).title("Writes")
          );

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/cluster/",
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive,
          };
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: (any)) => {
            if (axis instanceof Metrics.Axis) {
              return m("", {style: "float:left"}, Components.Metrics.LineGraph.create(this.exec, axis));
            } else {
              let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
              let totalStats: Models.Proto.StatusMetrics = nodeStatuses.totalStatus();

              axis.title = axis.titleFn ? axis.titleFn(allStats) : axis.title;
              axis.visualizationArguments.data = axis.visualizationArguments.dataFn(allStats, totalStats);
              axis.virtualVisualizationElement =
                m.component(Visualizations.NumberVisualization, axis.visualizationArguments);
              axis.warning = () => {
                let warning: Error = nodeStatuses.error();
                return warning && warning.toString();
              };
              return m("", {style: "float:left"}, m.component(Visualizations.VisualizationWrapper, axis));
            }
          }));
        }

        public RenderGraphsSmall(): MithrilElement {
          return m(".charts.half", this.axesSmall.map((axis: (any)) => {
            if (axis instanceof Metrics.Axis) {
              axis.legend(false).xAxis(false);
              return m(".small.half", {style: "float:left"}, Components.Metrics.LineGraph.create(this.exec, axis));
            } else {
              let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
              let totalStats: Models.Proto.StatusMetrics = nodeStatuses.totalStatus();

              axis.title = axis.titleFn ? axis.titleFn(allStats) : axis.title;
              axis.visualizationArguments.data = axis.visualizationArguments.dataFn(allStats, totalStats);
              axis.virtualVisualizationElement =
                m.component(Visualizations.NumberVisualization, axis.visualizationArguments);
              axis.warning = () => {
                let warning: Error = nodeStatuses.error();
                return warning && warning.toString();
              };
              return m(".small.half", {style: "float:left"},  m.component(Visualizations.VisualizationWrapper, axis));
            }
          }));
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
          let totalStats: Models.Proto.StatusMetrics = nodeStatuses.totalStatus();
          if (allStats) {
            return m(
              ".primary-stats.half",
              [
                {
                  title: "Nodes",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats.length },
                  },
                },
                {
                  title: "Total Ranges",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: totalStats[MetricNames.leaderRanges]},
                  },
                },
                {
                  title: "Available",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: totalStats[MetricNames.availableRanges] / totalStats[MetricNames.leaderRanges]},
                  },
                },
                {
                  title: "Fully Replicated",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: totalStats[MetricNames.replicatedRanges] / totalStats[MetricNames.leaderRanges]},
                  },
                },
              ].map(function (v: any): MithrilComponent<any> {
                v.virtualVisualizationElement =
                  m.component(Visualizations.NumberVisualization, v.visualizationArguments);
                return m.component(Visualizations.VisualizationWrapper, v);
              })
            );
          }
          return m(".primary-stats");
        }

        private _refresh(): void {
          this.exec.refresh();
          nodeStatuses.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }

        private _addChartSmall(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axesSmall.push(axis);
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): MithrilElement {
        let detail: string = m.route.param("detail");

        let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at ));

        let primaryContent: MithrilElement | MithrilElement[];
        if (detail === "events") {
          primaryContent = m(".section.table", m.component(Components.Events, 10));
        } else  {
          primaryContent = m(".section.overview", [
            ctrl.RenderGraphsSmall(),
            ctrl.RenderGraphs(),
          ]);
        }

        return m(".page.cluster", [
          m.component(Components.Topbar, {title: "Cluster", updated: mostRecentlyUpdated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          primaryContent,
        ]);
      }
    }
  }
}
