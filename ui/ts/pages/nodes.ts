// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
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
  export module Nodes {
    import Metrics = Models.Metrics;
    import Table = Components.Table;
    import NodeStatus = Models.Proto.NodeStatus;
    import Moment = moment.Moment;
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

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
    export module NodesPage {
      import MithrilComponent = _mithril.MithrilComponent;
      import bytesAndCountReducer = Models.Status.bytesAndCountReducer;
      import sumReducer = Models.Status.sumReducer;
      import BytesAndCount = Models.Status.BytesAndCount;

      class Controller {
        private static _queryEveryMS: number = 10000;

        private static comparisonColumns: Table.TableColumn<NodeStatus>[] = [
          {
            title: "",
            view: (status: NodeStatus): MithrilVirtualElement => {
              let lastUpdate: Moment = moment(Utils.Convert.NanoToMilli(status.stats.last_update_nanos));
              let s: string = Models.Status.staleStatus(lastUpdate);
              return m("div.status.icon-cockroach-15." + s); // icon 15 is a circle
            },
            sortable: true,
          },
          {
            title: "Node ID",
            view: (status: NodeStatus): MithrilElement =>
              m("a", {href: "/nodes/" + status.desc.node_id, config: m.route}, status.desc.node_id.toString()),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.desc.node_id,
            rollup: function(rows: NodeStatus[]): MithrilVirtualElement {
              interface StatusTotals {
                missing?: number;
                stale?: number;
                healthy?: number;
              }
              let statuses: StatusTotals = _.countBy(rows, (row: NodeStatus) => Models.Status.staleStatus(moment(Utils.Convert.NanoToMilli(row.stats.last_update_nanos))));

              return m("node-counts", [
                m("span.healthy", statuses.healthy || 0),
                m("span", "/"),
                m("span.stale", statuses.stale || 0),
                m("span", "/"),
                m("span.missing", statuses.missing || 0),
              ]);
            },
          },
          {
            title: "Address",
            view: (status: NodeStatus): string => status.desc.address.address,
            sortable: true,
          },
          {
            title: "Started At",
            view: (status: NodeStatus): string => {
              let date = new Date(Utils.Convert.NanoToMilli(status.started_at));
              return Utils.Format.Date(date);
            },
            sortable: true,
          },
          {
            title: "Live Bytes",
            view: (status: NodeStatus): string =>
              status.stats.live_count + " (" + Utils.Format.Bytes(status.stats.live_bytes) + ")",
            sortable: true,
            sortValue: (status: NodeStatus): number => status.stats.live_bytes,
            rollup: function(rows: NodeStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.live_bytes", "stats.live_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Key Bytes",
            view: (status: NodeStatus): string =>
              status.stats.key_count + " (" + Utils.Format.Bytes(status.stats.key_bytes) + ")",
            sortable: true,
            sortValue: (status: NodeStatus): number => status.stats.key_bytes,
            rollup: function(rows: NodeStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.key_bytes", "stats.key_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Value Bytes",
            view: (status: NodeStatus): string =>
              status.stats.val_count + " (" + Utils.Format.Bytes(status.stats.val_bytes) + ")",
            sortable: true,
            sortValue: (status: NodeStatus): number => status.stats.val_bytes,
            rollup: function(rows: NodeStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.val_bytes", "stats.val_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Intent Bytes",
            view: (status: NodeStatus): string =>
              status.stats.intent_count + " (" + Utils.Format.Bytes(status.stats.intent_bytes) + ")",
            sortable: true,
            sortValue: (status: NodeStatus): number => status.stats.intent_bytes,
            rollup: function(rows: NodeStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.intent_bytes", "stats.intent_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "System Bytes",
            view: (status: NodeStatus): string =>
              status.stats.sys_count + " (" + Utils.Format.Bytes(status.stats.sys_bytes) + ")",
            sortable: true,
            sortValue: (status: NodeStatus): number => status.stats.sys_bytes,
            rollup: function(rows: NodeStatus[]): string {
              let total: BytesAndCount = bytesAndCountReducer("stats.sys_bytes", "stats.sys_count", rows);
              return Utils.Format.Bytes(total.bytes) + " (" + total.count + ")";
            },
            section: "storage",
          },
          {
            title: "Leader Ranges",
            view: (status: NodeStatus): string => status.leader_range_count.toString(),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.leader_range_count,
            rollup: function(rows: NodeStatus[]): string {
              return sumReducer("leader_range_count", rows).toString();
            },
            section: "ranges",
          },
          {
            title: "Replicated Ranges",
            view: (status: NodeStatus): string => status.replicated_range_count.toString(),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.replicated_range_count,
            rollup: function(rows: NodeStatus[]): string {
              return sumReducer("replicated_range_count", rows).toString();
            },
            section: "ranges",
          },
          {
            title: "Available Ranges",
            view: (status: NodeStatus): string => status.available_range_count.toString(),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.available_range_count,
            rollup: function(rows: NodeStatus[]): string {
              return sumReducer("available_range_count", rows).toString();
            },
            section: "ranges",
          },
          {
            title: "Logs",
            view: (status: NodeStatus): MithrilElement =>
              m("a", { href: "/logs/" + status.desc.node_id, config: m.route }, "Log"),
          },
        ];

        public columns: Utils.Property<Table.TableColumn<NodeStatus>[]> = Utils.Prop(Controller.comparisonColumns);
        public sources: string[] = [];
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _interval: number;
        private _query: Metrics.Query;

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("ranges.available"))
                .sources(this.sources)
                .title("% Available Ranges")
              ));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.AvgRate(_nodeMetric("calls.error"))
                .sources(this.sources)
                .title("Error Calls")
              ));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("Live Bytes")
              ));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .sources(this.sources) // TODO: store sources vs node sources
                .title("CPU User %")
              ));

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.Status = nodeStatuses.totalStatus();
          if (allStats) {
            return m(".primary-stats", [
                {
                  title: "Total Ranges",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats.range_count},
                  },
                },
                {
                  title: "Total Live Bytes",
                  visualizationArguments: {
                    formatFn: function (v: number): string {
                      return Utils.Format.Bytes(v);
                    },
                    zoom: "50%",
                    data: {value: allStats.stats.live_bytes},
                  },
                },
                {
                  title: "Leader Ranges",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats.leader_range_count},
                  },
                },
                {
                  title: "Available",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: allStats.available_range_count / allStats.leader_range_count},
                  },
                },
                {
                  title: "Fully Replicated",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: allStats.replicated_range_count / allStats.leader_range_count},
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

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, Components.Metrics.LineGraph.create(this.exec, axis));
          }));
        }

        private _refresh(): void {
          this.exec.refresh();
          nodeStatuses.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
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
        let comparisonData: Table.TableData<NodeStatus> = {
          columns: ctrl.columns,
          rows: nodeStatuses.allStatuses,
        };

        let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at ));
        return m(".page", [
          m.component(Components.Topbar, {title: "Nodes", updated: mostRecentlyUpdated}),
          m(".section", [
            m(".subtitle", m("h1", "Node Overview")),
            ctrl.RenderGraphs(),
          ]),
          m(".section.table", m(".stats-table", Components.Table.create(comparisonData))),
        ]);
      }
    }

    /**
     * NodePage show the details of a single node.
     */
    export module NodePage {
      import NavigationBar = Components.NavigationBar;

      class Controller {
        private static defaultTargets: NavigationBar.Target[] = [
          {
            view: "Overview",
            route: "",
          },
          {
            view: "Graphs",
            route: "graph",
          },
        ];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          return ((m.route.param("detail") || "") === t.route);
        };

        private static _queryEveryMS: number = 10000;
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;
        private _nodeId: string;

        public onunload(): void {
          clearInterval(this._interval);
        }

        public constructor(nodeId: string) {
          this._nodeId = nodeId;
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.AvgRate(_nodeMetric("calls.success"))
                .sources([nodeId])
                .title("Successful Calls")
              )
              .label("Count / 10 sec."));
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.AvgRate(_nodeMetric("calls.error"))
                .sources([nodeId])
                .title("Error Calls")
              )
              .label("Count / 10 sec."));

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public RenderPrimaryStats(): MithrilElement {
          let nodeStats: Models.Proto.NodeStatus = nodeStatuses.GetStatus(this._nodeId);
          if (nodeStats) {
            return m(".primary-stats", [
              m(".stat", [
                m("span.title", "Started At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(nodeStats.started_at)))),
              ]),
              m(".stat", [
                m("span.title", "Last Updated At"),
                m("span.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(nodeStats.updated_at)))),
              ]),
              m(".stat", [
                m("span.title", "Total Ranges"),
                m("span.value", nodeStats.range_count),
              ]),
              m(".stat", [
                m("span.title", "Total Live Bytes"),
                m("span.value", Utils.Format.Bytes(nodeStats.stats.live_bytes)),
              ]),
              m(".stat", [
                m("span.title", "Leader Ranges"),
                m("span.value", nodeStats.leader_range_count),
              ]),
              m(".stat", [
                m("span.title", "Available"),
                m("span.value", Utils.Format.Percentage(nodeStats.available_range_count, nodeStats.leader_range_count)),
              ]),
              m(".stat", [
                m("span.title", "Fully Replicated"),
                m("span.value", Utils.Format.Percentage(nodeStats.replicated_range_count, nodeStats.leader_range_count)),
              ]),
            ]);
          }
          return m(".primary-stats");
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", this.axes.map((axis: Metrics.Axis) => {
            return m("", { style: "float:left" }, [
              m("h4", axis.title()),
              Components.Metrics.LineGraph.create(this.exec, axis),
            ]);
          }));
        }

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/nodes/" + this._nodeId + "/",
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive,
          };
        }

        public GetNodeId(): string {
          return this._nodeId;
        }

        private _refresh(): void {
          nodeStatuses.refresh();
          this.exec.refresh();
        }

        private _addChart(axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          this.axes.push(axis);
        }
      }

      export function controller(): Controller {
        let nodeId: string = m.route.param("node_id");
        return new Controller(nodeId);
      }

      export function view(ctrl: Controller): MithrilElement {
        let detail: string = m.route.param("detail");

        // Page title.
        let title: string = "Nodes: Node " + ctrl.GetNodeId();
        if (detail === "graph") {
          title += ": Graphs";
        }

        // Primary content
        let primaryContent: MithrilElement;
        if (detail === "graph") {
          primaryContent = ctrl.RenderGraphs();
        } else {
          primaryContent = ctrl.RenderPrimaryStats();
        }

        let nodeStatus: NodeStatus = nodeStatuses.GetStatus(ctrl.GetNodeId());
        let updated: number = (nodeStatus ? nodeStatus.updated_at : 0);

        return m(".page", [
          m.component(Components.Topbar, {title: title, updated: updated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          m(".section", primaryContent),
        ]);
      }
    }
  }
}
