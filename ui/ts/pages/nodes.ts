// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts"/>
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
  import NavigationBar = Components.NavigationBar;
  import Metrics = Models.Metrics;
  import Table = Components.Table;
  import NodeStatus = Models.Proto.NodeStatus;
  import Moment = moment.Moment;
  import MithrilVirtualElement = _mithril.MithrilVirtualElement;
  import MithrilComponent = _mithril.MithrilComponent;
  import bytesAndCountReducer = Models.Status.bytesAndCountReducer;
  import sumReducer = Models.Status.sumReducer;
  import BytesAndCount = Models.Status.BytesAndCount;
  import StoreStatus = Models.Proto.StoreStatus;

  /**
   * Nodes is the view for exploring the status of all nodes.
   */
  export module Nodes {
    let nodeStatuses: Models.Status.Nodes = new Models.Status.Nodes();
    let storeStatuses: Models.Status.Stores = new Models.Status.Stores();

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

      function ByteColumn(key: string, section: string, title?: string): Table.TableColumn<NodeStatus> {
        let byteKey: string = key + "_bytes";
        let countKey: string = key + "_count";
        return {
          title: title || Utils.Format.Titlecase(_.last(byteKey.split("."))),
          view: (status: NodeStatus): MithrilVirtualElement => Table.FormatBytes(parseFloat(_.get<string>(status, byteKey))),
          sortable: true,
          sortValue: (status: NodeStatus): number => parseFloat(_.get<string>(status, byteKey)),
          rollup: function(rows: NodeStatus[]): MithrilVirtualElement {
            let total: BytesAndCount = bytesAndCountReducer(byteKey, countKey, rows);
            return Table.FormatBytes(total.bytes);
          },
          section: section,
        };
      }

      function nodeId(s: NodeStatus): number {
        return s.desc.node_id;
      }

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

        private static comparisonColumns: Table.TableColumn<NodeStatus>[] = [
          {
            title: "",
            view: (status: NodeStatus): MithrilVirtualElement => {
              let lastUpdate: Moment = moment(Utils.Convert.NanoToMilli(status.stats.last_update_nanos));
              let s: string = Models.Status.staleStatus(lastUpdate);
              return m("div.status.icon-circle-filled." + s);
            },
            sortable: true,
          },
          {
            title: "Node ID",
            view: (status: NodeStatus): MithrilElement =>
              m("a", {href: "/nodes/" + nodeId(status), config: m.route}, nodeId(status) ? nodeId(status).toString() : null),
            sortable: true,
            sortValue: (status: NodeStatus): number => nodeId(status),
            rollup: function(rows: NodeStatus[]): MithrilVirtualElement {
              interface StatusTotals {
                missing?: number;
                stale?: number;
                healthy?: number;
              }
              let statuses: StatusTotals = _.countBy(_.filter(rows, nodeId), (row: NodeStatus) => Models.Status.staleStatus(moment(Utils.Convert.NanoToMilli(row.stats.last_update_nanos))));

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
            title: "Stores",
            view: (status: NodeStatus): MithrilElement => (<NodeStatus>status).store_ids.length,
            sortable: true,
            sortValue: (status: NodeStatus): number => (<NodeStatus>status).store_ids.length,
            rollup: function(rows: NodeStatus[]): string {
              return sumReducer("store_ids.length", <NodeStatus[]>rows).toString();
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
          ByteColumn("stats.live", "storage"),
          ByteColumn("stats.key", "storage"),
          ByteColumn("stats.val", "storage", "Value Bytes"),
          ByteColumn("stats.intent", "storage"),
          ByteColumn("stats.sys", "storage", "System Bytes"),
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
              m("a", { href: "/logs/" + (nodeId(status) ? nodeId(status) : ""), config: m.route }, nodeId(status) ? "Logs" : ""),
          },
        ];

        public columns: Utils.Property<Table.TableColumn<NodeStatus>[]> = Utils.Prop(Controller.comparisonColumns);
        exec: Metrics.Executor;
        axes: Metrics.Axis[] = [];
        private _interval: number;
        private _query: Metrics.Query;

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
            return ((m.route.param("detail") || "") === t.route);
        };

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();
          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("ranges.available"))
                .title("Available Ranges")
              ).format(d3.format("d")));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("exec.error-count"))
                .nonNegativeRate()
                .title("Error Calls")
              ).format(d3.format("d")));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .title("Live Bytes")
              ).format(Utils.Format.Bytes));

          this._addChart(
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .title("CPU User %")
              ).format(d3.format(".2%")));

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

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/nodes/",
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive,
          };
        }

        private _refresh(): void {
          this.exec.refresh();
          nodeStatuses.refresh();
          storeStatuses.refresh();
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

        let comparisonData: Table.TableData<NodeStatus> = {
          columns: ctrl.columns,
          rows: nodeStatuses.allStatuses,
        };

        let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at ));
        return m(".page", [
          m.component(Components.Topbar, {title: "Nodes", updated: mostRecentlyUpdated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
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

        private static _queryEveryMS: number = 10000;

        exec: Metrics.Executor;
        private networkAxes: Metrics.Axis[] = [];
        private sqlAxes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;
        private _nodeId: string;

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          return ((m.route.param("detail") || "") === t.route);
        };

        public onunload(): void {
          clearInterval(this._interval);
        }

        public constructor(nodeId: string) {
          this._nodeId = nodeId;
          this._query = Metrics.NewQuery();

          // Network stats.
          this._addChart(
            this.networkAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.conns"))
                .sources([nodeId])
                .title("Client Connections")
              )
              .label("Count"));
          this._addChart(
            this.networkAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.bytesin"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("Client Bytes In")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.networkAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.bytesout"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("Client Bytes Out")
              )
              .label("Count / 10 sec."));

          // Add SQL charts.
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("SELECTs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.update.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("UPDATEs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.insert.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("INSERTs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.delete.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("DELETEs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.begin.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("BEGINs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.commit.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("COMMITs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.rollback.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("ROLLBACKs")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.abort.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("Aborted Transactions")
              )
              .label("Count / 10 sec."));
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.ddl.count"))
                .nonNegativeRate()
                .sources([nodeId])
                .title("DDL Statements")
              )
              .label("Count / 10 sec."));

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public RenderPrimaryStats(): MithrilElement {
          let nodeStats: Models.Proto.NodeStatus = nodeStatuses.GetStatus(this._nodeId);
          if (nodeStats) {
            return m(".section.node-info", [
              m(".header", m("h1", `Node ${this._nodeId}`)),
              m("table.stats-table", m("tbody", [
                m("tr.stat", [
                  m("td.title", "Started At"),
                  m("td.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(nodeStats.started_at)))),
                ]),
                m("tr.stat", [
                  m("td.title", "Last Updated At"),
                  m("td.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(nodeStats.updated_at)))),
                ]),
                m("tr.stat", [
                  m("td.title", "Total Ranges"),
                  m("td.value", nodeStats.range_count),
                ]),
                m("tr.stat", [
                  m("td.title", "Total Live Bytes"),
                  m("td.value", Utils.Format.Bytes(nodeStats.stats.live_bytes)),
                ]),
                m("tr.stat", [
                  m("td.title", "Leader Ranges"),
                  m("td.value", nodeStats.leader_range_count),
                ]),
                m("tr.stat", [
                  m("td.title", "Available"),
                  m("td.value", Utils.Format.Percentage(nodeStats.available_range_count, nodeStats.leader_range_count)),
                ]),
                m("tr.stat", [
                  m("td.title", "Fully Replicated"),
                  m("td.value", Utils.Format.Percentage(nodeStats.replicated_range_count, nodeStats.leader_range_count)),
                ]),
              ])),
            ]
              .concat(_.map(nodeStats.store_ids, (id: number): MithrilVirtualElement => {
              let storeStatus: StoreStatus = storeStatuses.GetStatus(id.toString());
              if (storeStatus) {
                return m(".section.store-info", [
                  m(".header", m("h1", `Store ${id}`)),
                  m("table.stats-table", m("tbody", [
                    m("tr.stat", [
                      m("td.title", "Started At"),
                      m("td.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(storeStatus.started_at)))),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Last Updated At"),
                      m("td.value", Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(storeStatus.updated_at)))),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Total Ranges"),
                      m("td.value", storeStatus.range_count),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Total Live Bytes"),
                      m("td.value", Utils.Format.Bytes(storeStatus.stats.live_bytes)),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Leader Ranges"),
                      m("td.value", storeStatus.leader_range_count),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Available"),
                      m("td.value", Utils.Format.Percentage(storeStatus.available_range_count, storeStatus.leader_range_count)),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Fully Replicated"),
                      m("td.value", Utils.Format.Percentage(storeStatus.replicated_range_count, storeStatus.leader_range_count)),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Available Capacity"),
                      m("td.value", Utils.Format.Bytes(storeStatus.desc.capacity.available)),
                    ]),
                    m("tr.stat", [
                      m("td.title", "Total Capacity"),
                      m("td.value", Utils.Format.Bytes(storeStatus.desc.capacity.capacity)),
                    ]),
                  ])),
                ]);
              }
              return m(".primary-stats");
            }))
            );
          }
          return m(".primary-stats");
        }

        public RenderGraphs(): MithrilElement {
          return m(".charts", [
            m("h2", "Network Stats"),
            this.networkAxes.map((axis: Metrics.Axis) => {
              return m("", { style: "float:left" }, [
                Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
            }),
            m("h2", "SQL Queries"),
            this.sqlAxes.map((axis: Metrics.Axis) => {
              return m("", { style: "float:left" }, [
                Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
            }),
          ]);
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
          storeStatuses.refresh();
          this.exec.refresh();
        }

        private _addChart(axes: Metrics.Axis[], axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          axes.push(axis);
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
