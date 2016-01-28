// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/lodash/lodash.d.ts"/>
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../components/topbar.ts" />
/// <reference path="../util/format.ts" />
/// <reference path="../components/visualizations/visualizations.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  import MithrilElement = _mithril.MithrilVirtualElement;

  /**
   * Nodes is the view for exploring the status of all nodes.
   */
  export module Cluster {
    import Metrics = Models.Metrics;
    import NodeStatus = Models.Proto.NodeStatus;
    import Table = Components.Table;
    import LogEntry = Models.Proto.LogEntry;
    import Selector = Models.Metrics.Select.Selector;
    import MithrilComponent = _mithril.MithrilComponent;
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    let entries: Models.Log.Entries;

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
     * NodesPage show a list of all the available nodes.
     */
    export module Page {

      class Controller {
        private static _queryEveryMS: number = 10000;

        private static comparisonColumns: Table.TableColumn<LogEntry>[] = [
          {
            title: "Time",
            view: (entry: LogEntry): string => {
              let date = new Date(Utils.Convert.NanoToMilli(entry.time));
              return Utils.Format.Date(date);
            },
            sortable: true,
          },
          {
            title: "Severity",
            view: (entry: LogEntry): string => Utils.Format.Severity(entry.severity),
          },
          {
            title: "Message",
            view: (entry: LogEntry): string => Utils.Format.LogEntryMessage(entry),
          },
          {
            title: "Node",
            view: (entry: LogEntry): string => entry.node_id ? entry.node_id.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.node_id,
          },
          {
            title: "Store",
            view: (entry: LogEntry): string => entry.store_id ? entry.store_id.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.store_id,
          },
          {
            title: "Range",
            view: (entry: LogEntry): string => entry.range_id ? entry.range_id.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.range_id,
          },
          {
            title: "Key",
            view: (entry: LogEntry): string => entry.key,
            sortable: true,
          },
          {
            title: "File:Line",
            view: (entry: LogEntry): string => entry.file + ":" + entry.line,
            sortable: true,
          },
          {
            title: "Method",
            view: (entry: LogEntry): string => entry.method ? entry.method.toString() : "",
            sortable: true,
            sortValue: (entry: LogEntry): number => entry.method,
          },
        ];

        public columns: Utils.Property<Table.TableColumn<LogEntry>[]> = Utils.Prop(Controller.comparisonColumns);
        public sources: string[] = [];
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
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
          let thiz: Controller = this;

          let latencySelectors: Selector[] = _.map(
            this._quantiles,
            function(q: string): Selector {
              return Metrics.Select.Avg(_nodeMetric("exec.latency-1m" + q))
                .sources(thiz.sources)
                .title("Latency" + q);
            },
            this);
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
              ).format(d3.format(".2%")).title("CPU").range([0, 1]).stacked(true));

          entries = new Models.Log.Entries();
          entries.node(m.route.param("node_id") || "1");
          entries.level(m.route.param("level") || Utils.Format.Severity(0));
          entries.max(parseInt(m.route.param("max"), 10) || null);
          entries.startTime(parseInt(m.route.param("startTime"), 10) || null);
          entries.endTime(parseInt(m.route.param("endTime"), 10) || null);
          entries.pattern(m.route.param("pattern") || null);

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, Components.Metrics.LineGraph.create(this.exec, axis));
          }));
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.NodeStatus[] = nodeStatuses.allStatuses();
          let totalStats: Models.Proto.Status = nodeStatuses.totalStatus();
          if (allStats) {
            return m(".primary-stats.half", [
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
          entries.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }
      }

      //////// FROM LOGS page

        const _severitySelectOptions: Components.Select.Item[] = [
          { value: Utils.Format.Severity(0), text: ">= " + Utils.Format.Severity(0) },
          { value: Utils.Format.Severity(1), text: ">= " + Utils.Format.Severity(1) },
          { value: Utils.Format.Severity(2), text: ">= " + Utils.Format.Severity(2) },
          { value: Utils.Format.Severity(3), text: Utils.Format.Severity(3) },
        ];

        function onChangeSeverity(val: string): void {
          entries.level(val);
          m.route(entries.getURL(), entries.getParams());
        };

        function onChangeMax(val: string): void {
          let result: number = parseInt(val, 10);
          if (result > 0) {
            entries.max(result);
          } else {
            entries.max(null);
          }
          m.route(entries.getURL(), entries.getParams());
        }

        function onChangePattern(val: string): void {
          entries.pattern(val);
          m.route(entries.getURL(), entries.getParams());
        }

      ////// END FROM LOGS PAGE

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

        let comparisonData: Table.TableData<LogEntry> = {
          columns: ctrl.columns,
          rows: entries.allEntries,
        };
        let count: number;
        if (entries.allEntries()) {
          count = entries.allEntries().length;
        } else {
          count = 0;
        }

        let logs: MithrilVirtualElement = m(".section.table", [
          m("h2", "Node " + entries.nodeName() + " Log"),
          m("form", [
            m.trust("Severity: "),
            m.component(Components.Select, {
              items: _severitySelectOptions,
              value: entries.level,
              onChange: onChangeSeverity,
            }),
            m("span", "Max Results: "),
            m("input", { onchange: m.withAttr("value", onChangeMax), value: entries.max() }),
            m("span", "Regex Filter: "),
            m("input", { onchange: m.withAttr("value", onChangePattern), value: entries.pattern() }),
          ]),
          m("p", count + " log entries retrieved"),
          m(".stats-table", Components.Table.create(comparisonData)),
        ]);

        return m(".page", [
          m.component(Components.Topbar, {title: "CockroachDB Cluster", updated: mostRecentlyUpdated}),
          m(".section", [
            m(".subtitle", m("h1", "Cluster Overview")),
            ctrl.RenderPrimaryStats(),
            ctrl.RenderGraphs(),
          ]),
          logs,
        ]);
      }
    }
  }
}
