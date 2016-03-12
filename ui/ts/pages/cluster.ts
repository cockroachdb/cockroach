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

    let nodeStatuses: Models.Status.Nodes = new Models.Status.Nodes();

    function _nodeMetric(metric: string): string {
      return "cr.node." + metric;
    }

    function _storeMetric(metric: string): string {
      return "cr.store." + metric;
    }

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

        public sources: string[] = [];
        exec: Metrics.Executor;
        axes: (any)[] = [];
        axesSmall: (any)[] = [];
        private _interval: number;
        private _query: Metrics.Query;

        private _quantiles: string[] = [
          "-max",
          "-p99.999",
          "-p99.99",
          "-p99.9",
          "-p99",
          "-p90",
          "-p75",
          "-p50",
        ];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
            return ((m.route.param("detail") || "") === t.route);
        };

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();

          let latencySelectors: Selector[] = _.map(
            this._quantiles,
            (q: string): Selector => {
              return Metrics.Select.Avg(_nodeMetric("exec.latency-1m" + q))
                .title("Latency" + q);
            });
          this._addChart(Metrics.NewAxis.apply(this, latencySelectors)
          .format(Utils.Convert.NanoToMilli)
          .title("Latency (ms)")
          .label("Milliseconds")
          .range([0]));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("exec.error-count"))
                .nonNegativeRate()
                .title("Error Calls"),
              Metrics.Select.Avg(_nodeMetric("exec.success-count"))
                .nonNegativeRate()
                .title("Success Calls")
              ).format(d3.format("d")).title("Successes vs Errors").range([0]));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .title("Live Bytes"),
              Metrics.Select.Avg(_storeMetric("capacity.available"))
                .title("Available Capacity")
              ).format(Utils.Format.Bytes).title("Capacity").range([0]));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .title("CPU User %"),
              Metrics.Select.Avg(_sysMetric("cpu.sys.percent"))
                .title("CPU Sys %")
              ).format(d3.format(".2%")).title("CPU").range([0, 1]).stacked(true)
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.bytesin"))
                .nonNegativeRate()
                .title("Bytes In"),
              Metrics.Select.Avg(_nodeMetric("sql.bytesout"))
                .nonNegativeRate()
                .title("Bytes Out")
            ).format(Utils.Format.Bytes).title("data")
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.begin.count"))
                .nonNegativeRate()
                .title("BEGIN"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.commit.count"))
                .nonNegativeRate()
                .title("COMMIT"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.rollback.count"))
                .nonNegativeRate()
                .title("ROLLBACK"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.abort.count"))
                .nonNegativeRate()
                .title("ABORT")
            ).format(d3.format("d")).title("Transaction Info")
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.update.count"))
                .nonNegativeRate()
                .title("Updates"),
              Metrics.Select.Avg(_nodeMetric("sql.insert.count"))
                .nonNegativeRate()
                .title("Inserts"),
              Metrics.Select.Avg(_nodeMetric("sql.delete.count"))
                .nonNegativeRate()
                .title("Deletes"),
              Metrics.Select.Avg(_nodeMetric("sql.ddl.count"))
                .nonNegativeRate()
                .title("DDL")
            ).format(d3.format("d")).title("SQL Writes")
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .nonNegativeRate()
                .title("Selects")
            ).format(d3.format("d")).title("SQL Reads")
          );

          this.axesSmall.push({
            titleFn: (allStats: Models.Proto.NodeStatus[]): string => {
              if (allStats && allStats.length === 1) {
                return "Node";
              }
              return "Nodes";
            },
            visualizationArguments: {
              format: "s",
              dataFn: function (allStats: Models.Proto.NodeStatus[], totalStats: Models.Proto.Status): { value: number; } { return {value: allStats && allStats.length || 0 }; },
            },
          });

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
              let totalStats: Models.Proto.Status = nodeStatuses.totalStatus();

              axis.title = axis.titleFn(allStats);
              axis.visualizationArguments.data = axis.visualizationArguments.dataFn(allStats, totalStats);
              axis.virtualVisualizationElement =
                m.component(Visualizations.NumberVisualization, axis.visualizationArguments);
              return m("", {style: "float:left"}, m.component(Visualizations.VisualizationWrapper, axis));
            }
          }));
        }

        public RenderGraphsSmall(): MithrilElement {
          return m(".charts", this.axesSmall.map((axis: (any)) => {
            if (axis instanceof Metrics.Axis) {
              return m(".small", {style: "float:left"}, Components.Metrics.LineGraph.create(this.exec, axis));
            } else {
              let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
              let totalStats: Models.Proto.Status = nodeStatuses.totalStatus();

              axis.title = axis.titleFn(allStats);
              axis.visualizationArguments.data = axis.visualizationArguments.dataFn(allStats, totalStats);
              axis.virtualVisualizationElement =
                m.component(Visualizations.NumberVisualization, axis.visualizationArguments);
              return m(".small", {style: "float:left"},  m.component(Visualizations.VisualizationWrapper, axis));
            }
          }));
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
          let totalStats: Models.Proto.Status = nodeStatuses.totalStatus();
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
                    data: {value: totalStats.range_count},
                  },
                },
                {
                  title: "Available",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: totalStats.available_range_count / totalStats.leader_range_count},
                  },
                },
                {
                  title: "Fully Replicated",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: totalStats.replicated_range_count / totalStats.leader_range_count},
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
          Models.Events.eventSingleton.refresh();
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

        // set
        ctrl.sources = _.map(
          nodeStatuses.allStatuses(),
          function(v: NodeStatus): string {
            return v.desc.node_id.toString();
          }
        );

        let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at ));

        let primaryContent: MithrilElement | MithrilElement[];
        if (detail === "events") {
          primaryContent = m(".section.table", m.component(Components.Events, 10));
        } else  {
          primaryContent = [
            ctrl.RenderGraphs(),
            ctrl.RenderGraphsSmall(),
          ];
        }

        return m(".page", [
          m.component(Components.Topbar, {titleImage: m("img", {src: "assets/CockroachDB.png"}), updated: mostRecentlyUpdated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          m(".section", primaryContent),
        ]);
      }
    }
  }
}
