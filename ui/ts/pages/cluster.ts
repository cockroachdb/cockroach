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

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();

          let latencySelectors: Selector[] = _.map(
            this._quantiles,
            (q: string): Selector => {
              return Metrics.Select.Avg(_nodeMetric("exec.latency-1m" + q))
                .sources(this.sources)
                .title("Latency" + q);
            });
          this._addChart(Metrics.NewAxis.apply(this, latencySelectors)
          .format(Utils.Convert.NanoToMilli)
          .title("Latency (ms)")
          .label("Milliseconds")
          .range([0]));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.AvgRate(_nodeMetric("exec.error-count"))
                .sources(this.sources)
                .title("Error Calls"),
              Metrics.Select.AvgRate(_nodeMetric("exec.success-count"))
                .sources(this.sources)
                .title("Success Calls")
              ).format(d3.format("d")).title("Successes vs Errors").range([0]));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Live Bytes"),
              Metrics.Select.Avg(_storeMetric("capacity.available"))
                .sources(this.sources)
                .title("Available Capacity")
              ).format(Utils.Format.Bytes).title("Capacity").range([0]));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("CPU User %"),
              Metrics.Select.Avg(_sysMetric("cpu.sys.percent"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("CPU Sys %")
              ).format(d3.format(".2%")).title("CPU").range([0, 1]).stacked(true)
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("pgwire.bytesin"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Bytes In"),
              Metrics.Select.Avg(_nodeMetric("pgwire.bytesout"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Bytes Out")
            ).format(Utils.Format.Bytes).title("PGwire")
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.begin.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Transactions"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.commit.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Transactions"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.rollback.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Transactions"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.abort.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Transactions")
            ).format(d3.format("d")).title("Transactions")
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.update.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Updates"),
              Metrics.Select.Avg(_nodeMetric("sq.insert.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Inserts"),
              Metrics.Select.Avg(_nodeMetric("sq.delete.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Deletes"),
              Metrics.Select.Avg(_nodeMetric("sq.ddl.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("DDL")
            ).format(d3.format("d")).title("SQL Writes")
          );

          this._addChartSmall(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Selects")
            ).format(d3.format("d")).title("SQL Reads")
          );

          this.axesSmall.push({
            title: "Nodes",
            visualizationArguments: {
              format: ".0s",
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

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: (any)) => {
            if (axis instanceof Metrics.Axis) {
              return m("", {style: "float:left"}, Components.Metrics.LineGraph.create(this.exec, axis));
            } else {
              let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
              let totalStats: Models.Proto.Status = nodeStatuses.totalStatus();

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
        // set
        ctrl.sources = _.map(
          nodeStatuses.allStatuses(),
          function(v: NodeStatus): string {
            return v.desc.node_id.toString();
          }
        );

        let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at ));

        return m(".page", [
          m.component(Components.Topbar, {titleImage: m("img", {src: "assets/CockroachDB.png"}), updated: mostRecentlyUpdated}),
          m(".section", [
            m(".subtitle", m("h1", "Cluster Overview")),
            // ctrl.RenderPrimaryStats(),
            ctrl.RenderGraphs(),
            ctrl.RenderGraphsSmall(),
          ]),
          m(".section.table", m.component(Components.Events, 10)),
        ]);
      }
    }
  }
}
