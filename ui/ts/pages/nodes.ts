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
  import MetricNames = Models.Proto.MetricConstants;
  import Moment = moment.Moment;
  import MithrilVirtualElement = _mithril.MithrilVirtualElement;
  import MithrilComponent = _mithril.MithrilComponent;
  import sumReducer = Models.Status.sumReducer;

  /**
   * Nodes is the view for exploring the status of all nodes.
   */
  export module Nodes {
    let nodeStatuses: Models.Status.Nodes = Models.Status.nodeStatusSingleton;

    function _nodeMetric(metric: string): string {
      return "cr.node." + metric;
    }

    function _storeMetric(metric: string): string {
      return "cr.store." + metric;
    }

    function _sysMetric(metric: string): string {
      return "cr.node.sys." + metric;
    }

    function totalCpu(metrics: Models.Proto.StatusMetrics): number {
      return metrics[MetricNames.sysCPUPercent] + metrics[MetricNames.userCPUPercent];
    }

    export module NodeOverviewReroute {
      export function controller(): any {
        // passing in null to m.route.param() preserves the querystring
        m.route("/nodes/overview", m.route.param(null), true);
        return null;
      }

      export function view(ctrl: any): MithrilVirtualElement {
        return null;
      }
    }

    /**
     * NodesPage show a list of all the available nodes.
     */
    export module NodesPage {

      let aggregateByteKeys = [MetricNames.liveBytes, MetricNames.intentBytes, MetricNames.sysBytes];
      let byteSum = (s: NodeStatus): number => {
        return _.sumBy(aggregateByteKeys, (key: string) => {
          return s.metrics[key];
        });
      };

      function nodeId(s: NodeStatus): number {
        return s.desc.node_id;
      }

      class Controller {
        private static _queryEveryMS: number = 10000;

        private static defaultTargets: NavigationBar.Target[] = [
          {
            view: "Overview",
            route: "overview",
          },
          {
            view: "Graphs",
            route: "graphs",
          },
        ];

        private static comparisonColumns: Table.TableColumn<NodeStatus>[] = [
          {
            title: "",
            view: (status: NodeStatus): MithrilVirtualElement => {
              let lastUpdate: Moment = moment(Utils.Convert.NanoToMilli(status.updated_at));
              let s: string = Models.Status.staleStatus(lastUpdate);
              return m("div.status.icon-circle-filled." + s);
            },
            sortable: true,
          },
          {
            title: "Node",
            view: (status: NodeStatus): MithrilElement =>
              m("a", {href: "/nodes/" + nodeId(status), config: m.route}, status.desc.address.address),
            sortable: true,
            sortValue: (status: NodeStatus): string => status.desc.address.address,
            rollup: function(rows: NodeStatus[]): MithrilVirtualElement {
              interface StatusTotals {
                missing?: number;
                stale?: number;
                healthy?: number;
              }
              let statuses: StatusTotals = _.countBy(_.filter(rows, nodeId), (row: NodeStatus) => Models.Status.staleStatus(moment(Utils.Convert.NanoToMilli(row.updated_at))));

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
            title: "Started",
            view: (status: NodeStatus): string => {
              return moment((Utils.Convert.NanoToMilli(status.started_at))).fromNow();
            },
            sortable: true,
          },
          {
            title: "Bytes",
            view: (status: NodeStatus): MithrilVirtualElement => {
              return Table.FormatBytes(byteSum(status));
            },
            sortable: true,
            sortValue: (status: NodeStatus): number => byteSum(status),
              rollup: function(rows: NodeStatus[]): MithrilVirtualElement {
              return Table.FormatBytes(_.sumBy(rows, (row: NodeStatus): number => {
                return byteSum(row);
              }));
            },
          },
          {
            title: "Replicas",
            view: (status: NodeStatus): string => {
             return status.metrics[MetricNames.replicas].toString();
            },
            sortable: true,
            sortValue: (status: NodeStatus): number => status.metrics[MetricNames.replicas],
            rollup: function(rows: NodeStatus[]): string {
              return sumReducer(MetricNames.replicas, _.map(rows, (r: NodeStatus) => r.metrics)).toString();
            },
          },
          // TODO: add more stats
          {
            title: "Connections",
            view: (status: NodeStatus): string => {
              return status.metrics[MetricNames.sqlConns].toString();
            },
            sortable: true,
            sortValue: (status: NodeStatus): number => status.metrics[MetricNames.sqlConns],
            rollup: function(rows: NodeStatus[]): string {
              return _.sumBy(rows, (r: NodeStatus) => r.metrics[MetricNames.sqlConns]).toString();
            },
          },
          {
            title: "CPU Usage",
            view: (status: NodeStatus): string => {
              return d3.format(".2%")(totalCpu(status.metrics));
            },
            sortable: true,
            sortValue: (status: NodeStatus): number => totalCpu(status.metrics),
            rollup: function(rows: NodeStatus[]): string {
              return d3.format(".2%")(_.sumBy(rows, (r: NodeStatus) => totalCpu(r.metrics)) / rows.length);
            },
          },
          {
            title: "Mem Usage",
            view: (status: NodeStatus): string => {
              return Utils.Format.Bytes(status.metrics[MetricNames.rss]);
            },
            sortable: true,
            sortValue: (status: NodeStatus): number => status.metrics[MetricNames.rss],
            rollup: function(rows: NodeStatus[]): string {
              return Utils.Format.Bytes(_.sumBy(rows, (r: NodeStatus) => r.metrics[MetricNames.rss]));
            },
          },
          {
            title: "Logs",
            view: (status: NodeStatus): MithrilElement =>
              m("a", { href: "/nodes/" + nodeId(status) + "/logs" , config: m.route }, nodeId(status) ? "Logs" : ""),
          },
        ];

        public columns: Utils.Property<Table.TableColumn<NodeStatus>[]> = Utils.Prop(Controller.comparisonColumns);
        exec: Metrics.Executor;
        private activityAxes: Metrics.Axis[] = [];
        private sqlAxes: Metrics.Axis[] = [];
        private systemAxes: Metrics.Axis[] = [];
        private internalsAxes: Metrics.Axis[] = [];
        private _interval: number;
        private _query: Metrics.Query;

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          return _.startsWith(m.route(), "/nodes" + (t.route ? "/" + t.route : "" ));
        };

        public constructor(nodeId?: string) {
          this._query = Metrics.NewQuery();

          // General activity stats.
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.conns"))
                .title("Client Connections")
            ).format(d3.format(".1")).title("SQL Connections")
          );
          this._addChart(
            this.activityAxes,
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
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.query.count"))
                .nonNegativeRate()
                .title("Queries/sec")
            ).format(d3.format(".1")).title("Queries Per Second")
          );
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .title("Live Bytes")
              ).format(Utils.Format.Bytes));

          // SQL charts
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .nonNegativeRate()
                .title("Selects")
            ).format(d3.format(".1")).title("Reads")
           );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.update.count"))
                .nonNegativeRate()
                .title("Updates"),
              Metrics.Select.Avg(_nodeMetric("sql.insert.count"))
                .nonNegativeRate()
                .title("Inserts"),
              Metrics.Select.Avg(_nodeMetric("sql.delete.count"))
                .nonNegativeRate()
                .title("Deletes")
            ).format(d3.format(".1")).title("Writes")
          );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.commit.count"))
                .nonNegativeRate()
                .title("Commits"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.rollback.count"))
                .nonNegativeRate()
                .title("Rollbacks"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.abort.count"))
                .nonNegativeRate()
                .title("Aborts")
            ).format(d3.format(".1")).title("Transactions")
          );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.ddl.count"))
                .nonNegativeRate()
                .title("DDL Statements")
            ).format(d3.format(".1")).title("Schema Changes")
          );

          // System resource graphs
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .title("CPU User %"),
              Metrics.Select.Avg(_sysMetric("cpu.sys.percent"))
                .title("CPU Sys %")
            ).format(d3.format(".2%")).title("CPU Usage").stacked(true)
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("allocbytes"))
                .title("Go In Use"),
              Metrics.Select.Avg(_sysMetric("sysbytes"))
                .title("Go Sys"),
              Metrics.Select.Avg(_sysMetric("rss"))
                .title("RSS")
            ).format(Utils.Format.Bytes).title("Memory Usage")
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("goroutines"))
                .title("goroutine count")
            ).format(d3.format(".1")).title("goroutine Count")
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cgocalls"))
                .nonNegativeRate()
                .title("cgo Calls")
            ).format(d3.format(".1")).title("cgo Calls")
          );

          // Graphs for internals, such as RocksDB
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("txn.commits-count"))
                .nonNegativeRate()
                .title("Commits"),
              Metrics.Select.Avg(_nodeMetric("txn.commits1PC-count"))
                .nonNegativeRate()
                .title("Fast 1PC"),
              Metrics.Select.Avg(_nodeMetric("txn.aborts-count"))
                .nonNegativeRate()
                .title("Aborts"),
              Metrics.Select.Avg(_nodeMetric("txn.abandons-count"))
                .nonNegativeRate()
                .title("Abandons")
            ).format(d3.format(".1f"))
            .title("Key/Value Transactions")
            .label("transactions/sec")
            .stacked(true)
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.usage"))
                .title("Block Cache"),
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.pinned-usage"))
                .title("Iterators"),
              Metrics.Select.Avg(_storeMetric("rocksdb.memtable.total-size"))
                .title("Memtable")
            ).format(Utils.Format.Bytes).title("Engine Memory Usage")
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.hits"))
                .nonNegativeRate()
                .title("Cache Hits"),
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.misses"))
                .nonNegativeRate()
                .title("Cache Misses")
            ).format(d3.format(".1")).title("Block Cache Hits/Misses").stacked(true)
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("range.splits"))
                .nonNegativeRate()
                .title("Splits"),
              Metrics.Select.Avg(_storeMetric("range.adds"))
                .nonNegativeRate()
                .title("Adds"),
              Metrics.Select.Avg(_storeMetric("range.removes"))
                .nonNegativeRate()
                .title("Removes")
            ).format(d3.format(".1f"))
            .title("Range Events")
            .stacked(true)
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.flushes"))
                .nonNegativeRate()
                .title("Flushes"),
              Metrics.Select.Avg(_storeMetric("rocksdb.compactions"))
                .nonNegativeRate()
                .title("Compactions")
            ).format(d3.format(".1")).title("Flushes and Compactions")
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.bloom.filter.prefix.checked"))
                .nonNegativeRate()
                .title("Checked"),
              Metrics.Select.Avg(_storeMetric("rocksdb.bloom.filter.prefix.useful"))
                .nonNegativeRate()
                .title("Useful")
            ).format(d3.format(".1")).title("Bloom Filter Prefix")
          );
          let fmt: (v: number) => string = d3.format(".1f");
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("clock-offset.upper-bound-nanos"))
                .title("Upper Bound"),
              Metrics.Select.Avg(_nodeMetric("clock-offset.lower-bound-nanos"))
                .title("Lower Bound")
            ).format((v: number): string => fmt(Utils.Convert.NanoToMilli(v)))
            .title("Clock Offset")
            .label("Milliseconds")
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("gc.pause.ns"))
                .nonNegativeRate()
                .title("Time")
            ).format((v: number): string => fmt(Utils.Convert.NanoToMilli(v)))
            .title("GC Pause Time")
            .label("Milliseconds")
          );

          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public onunload(): void {
          clearInterval(this._interval);
        }

        public RenderPrimaryStats(): MithrilElement {
          let allStats: Models.Proto.StatusMetrics = nodeStatuses.totalStatus();
          if (allStats) {
            return m(".primary-stats", [
                {
                  title: "Total Replicas",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats[MetricNames.replicas]},
                  },
                },
                {
                  title: "Total Live Bytes",
                  visualizationArguments: {
                    formatFn: function (v: number): string {
                      return Utils.Format.Bytes(v);
                    },
                    zoom: "50%",
                    data: {value: allStats[MetricNames.liveBytes]},
                  },
                },
                {
                  title: "Leader Ranges",
                  visualizationArguments: {
                    format: ".0s",
                    data: {value: allStats[MetricNames.leaderRanges]},
                  },
                },
                {
                  title: "Available",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: allStats[MetricNames.availableRanges] / allStats[MetricNames.leaderRanges]},
                  },
                },
                {
                  title: "Fully Replicated",
                  visualizationArguments: {
                    format: "3%",
                    data: {value: allStats[MetricNames.replicatedRanges] / allStats[MetricNames.leaderRanges]},
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
          return m(".section.nodes", [
            m(".charts", [
              m("h2", "Activity"),
              this.activityAxes.map((axis: Metrics.Axis) => {
                return m("", { style: "float:left" }, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
              m("h2", "SQL Queries"),
              this.sqlAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
              m("h2", "System Resources"),
              this.systemAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
              m("h2", "Internals"),
              this.internalsAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
            ]),
          ]);
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
        }

        private _addChart(axes: Metrics.Axis[], axis: Metrics.Axis): void {
          axis.selectors().forEach((s: Metrics.Select.Selector) => this._query.selectors().push(s));
          axes.push(axis);
        }
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): MithrilElement {
        let isOverview: boolean = _.startsWith(m.route(), "/nodes/overview");
        let primaryContent: MithrilElement;

        if (isOverview) {
          // Node overview (nodes table).
          let comparisonData: Table.TableData<NodeStatus> = {
            columns: ctrl.columns,
            rows: nodeStatuses.allStatuses,
          };
          primaryContent =
            m(".section.table.node-overview",
              m(".stats-table", Components.Table.create(comparisonData)));
        } else {
          // Graphs for all nodes.
          primaryContent = ctrl.RenderGraphs();
        }

        let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at));
        return m(".page", [
          m.component(Components.Topbar, {title: "Nodes", updated: mostRecentlyUpdated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          primaryContent,
        ]);
      }
    }

    /**
     * NodePage show the details of a single node.
     */
    export module NodePage {
      import Status = Models.Proto.StatusMetrics;
      import MetricNames = Models.Proto.MetricConstants;
      export class Controller {
        // used by the logs page as well
        public static nodeTabs: NavigationBar.Target[] = [
          {
            view: "Overview",
            route: "",
          },
          {
            view: "Graphs",
            route: "graph",
          },
          {
            view: "Logs",
            route: "logs",
          },
        ];

        private static _queryEveryMS: number = 10000;

        exec: Metrics.Executor;
        private activityAxes: Metrics.Axis[] = [];
        private sqlAxes: Metrics.Axis[] = [];
        private systemAxes: Metrics.Axis[] = [];
        private internalsAxes: Metrics.Axis[] = [];
        private _query: Metrics.Query;
        private _interval: number;
        private _nodeId: string;
        private _storeIds: string[] = [];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          return ((m.route.param("detail") || "") === t.route);
        };

        public onunload(): void {
          clearInterval(this._interval);
        }

        public constructor(nodeId: string) {
          this._nodeId = nodeId;
          this._initGraphs();
          this.exec = new Metrics.Executor(this._query);
          this._refresh();
          this._interval = window.setInterval(() => this._refresh(), Controller._queryEveryMS);
        }

        public RenderPrimaryStats(): MithrilElement {
          let nodeStats: Models.Proto.NodeStatus = nodeStatuses.GetStatus(this._nodeId);

          // CellInfo captures the human readable title and accessor function for a specific Store/Node status value
          interface CellInfo {
            title: string;
            valueFn: (s: Status) => string;
          }

          let values: CellInfo[] = [
            {title: "Live Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.liveBytes])},
            {title: "Key Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.keyBytes])},
            {title: "Value Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.valBytes])},
            {title: "Intent Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.intentBytes])},
            {title: "Sys Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.sysBytes])},
            {title: "GC Bytes Age", valueFn: (s: Status): string => s[MetricNames.gcBytesAge].toString()},
            {title: "Total Replicas", valueFn: (s: Status): string => s[MetricNames.replicas].toString()},
            {title: "Leader Ranges", valueFn: (s: Status): string => s[MetricNames.leaderRanges].toString()},
            {title: "Available", valueFn: (s: Status): string => Utils.Format.Percentage(s[MetricNames.availableRanges], s[MetricNames.leaderRanges])},
            {title: "Fully Replicated", valueFn: (s: Status): string => Utils.Format.Percentage(s[MetricNames.replicatedRanges], s[MetricNames.leaderRanges])},
            {title: "Available Capacity", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.availableCapacity])},
            {title: "Total Capacity", valueFn: (s: Status): string => Utils.Format.Bytes(s[MetricNames.capacity])},
          ];

          // genCells generates an array of virtual table cells populated with values from each node store
          // the only argument is the valueFn from CellInfo
          function genCells(valueFn: (s: Status) => string): MithrilVirtualElement[] {
            return _.map(nodeStats.store_statuses, (storeStatus: Models.Proto.StoreStatus): MithrilVirtualElement => {
              return m("td.value", valueFn(storeStatus.metrics));
            });
          }

          // genRow generates the virtual table row from a CellInfo object
          function genRow(info: CellInfo): MithrilVirtualElement {
            return m("tr", [
              m("td.title", info.title),              // First cell: the human readable title for the row
              m("td.value", info.valueFn(nodeStats.metrics)), // Second cell: the node value
            ].concat(genCells(info.valueFn)));        // Third and successive cells: the store values
          }

          if (nodeStats) {
            return m(".section.table", [
              m(".stats-table", m("table", [
                m("thead",
                  // header row
                  m("tr", [
                    m("th.title", ""),
                    m("th.value", `Node ${this._nodeId}`), // node name header cell
                    // add the name each store into this row by concatenating the generated cells onto this array
                  ].concat(_.map(nodeStats.store_statuses, (s): MithrilVirtualElement => m("th.value", `Store ${s.desc.store_id}`))))),
                m("tbody", _.map(values, genRow)),
              ])),
            ]);
          }
          return m(".primary-stats");
        }

        public RenderBuildInfo(): MithrilElement {
          let nodeStats: Models.Proto.NodeStatus = nodeStatuses.GetStatus(this._nodeId);
          if (nodeStats) {
            return m(".section.info", [
              m(".info", "Build: " + nodeStats.build_info.tag),
            ]);
          }
          return null;
        }

        public RenderGraphs(): MithrilElement {
          return m("", [
            m(".charts", [
              m("h2", "Client Activity"),
              this.activityAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
              m("h2", "SQL Queries"),
              this.sqlAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
              m("h2", "System Resources"),
              this.systemAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
              m("h2", "Internals"),
              this.internalsAxes.map((axis: Metrics.Axis) => {
                return m("", {style: "float:left"}, [
                  Components.Metrics.LineGraph.create(this.exec, axis),
                ]);
              }),
            ]),
          ]);
        }

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "/nodes/" + this._nodeId + "/",
            targets: Utils.Prop(Controller.nodeTabs),
            isActive: Controller.isActive,
          };
        }

        public GetNodeId(): string {
          return this._nodeId;
        }

        private _initGraphs(): void {
          this._query = Metrics.NewQuery();
          this.activityAxes = [];
          this.sqlAxes = [];
          this.systemAxes = [];
          this.internalsAxes = [];

          // General activity stats
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.conns"))
                .sources([this._nodeId])
                .title("Client Connections")
            ).format(d3.format(".1")).title("SQL Connections")
          );
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.bytesin"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Bytes In"),
              Metrics.Select.Avg(_nodeMetric("sql.bytesout"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Bytes Out")
            ).format(Utils.Format.Bytes).title("SQL Traffic")
          );
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.query.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Queries/sec")
            ).format(d3.format(".1")).title("Queries Per Second")
          );
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("livebytes"))
                .sources([this._nodeId])
                .title("Live Bytes")
              ).format(Utils.Format.Bytes));

          // SQL charts
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Selects")
            ).format(d3.format(".1")).title("Reads")
           );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.update.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Updates"),
              Metrics.Select.Avg(_nodeMetric("sql.insert.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Inserts"),
              Metrics.Select.Avg(_nodeMetric("sql.delete.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Deletes")
            ).format(d3.format(".1")).title("Writes")
          );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.txn.commit.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Commits"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.rollback.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Rollbacks"),
              Metrics.Select.Avg(_nodeMetric("sql.txn.abort.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Aborts")
            ).format(d3.format(".1")).title("Transactions")
          );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.ddl.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("DDL Statements")
            ).format(d3.format(".1")).title("Schema Changes")
          );

          // System resource graphs
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cpu.user.percent"))
                .sources([this._nodeId])
                .title("CPU User %"),
              Metrics.Select.Avg(_sysMetric("cpu.sys.percent"))
                .sources([this._nodeId])
                .title("CPU Sys %")
            ).format(d3.format(".2%")).title("CPU Usage").stacked(true)
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("allocbytes"))
                .sources([this._nodeId])
                .title("Go In Use"),
              Metrics.Select.Avg(_sysMetric("sysbytes"))
                .sources([this._nodeId])
                .title("Go Sys"),
              Metrics.Select.Avg(_sysMetric("rss"))
                .sources([this._nodeId])
                .title("RSS")
            ).format(Utils.Format.Bytes).title("Memory Usage")
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("goroutines"))
                .sources([this._nodeId])
                .title("goroutine count")
            ).format(d3.format(".1")).title("goroutine Count")
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cgocalls"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("cgo Calls")
            ).format(d3.format(".1")).title("cgo Calls")
          );

          // Graphs for internals, such as RocksDB
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("txn.commits-count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Commits"),
              Metrics.Select.Avg(_nodeMetric("txn.commits1PC-count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Fast 1PC"),
              Metrics.Select.Avg(_nodeMetric("txn.aborts-count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Aborts"),
              Metrics.Select.Avg(_nodeMetric("txn.abandons-count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Abandons")
            ).format(d3.format(".1f"))
            .title("Key/Value Transactions")
            .label("transactions/sec")
            .stacked(true)
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.usage"))
                .sources(this._storeIds)
                .title("Block Cache"),
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.pinned-usage"))
                .sources(this._storeIds)
                .title("Iterators"),
              Metrics.Select.Avg(_storeMetric("rocksdb.memtable.total-size"))
                .sources(this._storeIds)
                .title("Memtable")
            ).format(Utils.Format.Bytes).title("Engine Memory Usage")
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.hits"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Cache Hits"),
              Metrics.Select.Avg(_storeMetric("rocksdb.block.cache.misses"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Cache Misses")
            ).format(d3.format(".1")).title("Block Cache Hits/Misses").stacked(true)
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("range.splits"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Splits"),
              Metrics.Select.Avg(_storeMetric("range.adds"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Adds"),
              Metrics.Select.Avg(_storeMetric("range.removes"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Removes")
            ).format(d3.format(".1f"))
            .title("Range Events")
            .stacked(true)
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.flushes"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Flushes"),
              Metrics.Select.Avg(_storeMetric("rocksdb.compactions"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Compactions")
            ).format(d3.format(".1")).title("Flushes and Compactions")
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_storeMetric("rocksdb.bloom.filter.prefix.checked"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Checked"),
              Metrics.Select.Avg(_storeMetric("rocksdb.bloom.filter.prefix.useful"))
                .sources(this._storeIds)
                .nonNegativeRate()
                .title("Useful")
            ).format(d3.format(".1")).title("Bloom Filter Prefix")
          );
          let fmt: (v: number) => string = d3.format(".1f");
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("clock-offset.upper-bound-nanos"))
                .sources([this._nodeId])
                .title("Upper Bound"),
              Metrics.Select.Avg(_nodeMetric("clock-offset.lower-bound-nanos"))
                .sources([this._nodeId])
                .title("Lower Bound")
            ).format((v: number): string => fmt(Utils.Convert.NanoToMilli(v)))
            .title("Clock Offset")
            .label("Milliseconds")
          );
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("gc.pause.ns"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Time")
            ).format((v: number): string => fmt(Utils.Convert.NanoToMilli(v)))
            .title("GC Pause Time")
            .label("Milliseconds")
          );
        }

        private _refresh(): void {
          nodeStatuses.refresh();

          // When our NodeStatus has been retrieved, update our store_ids so
          // that graphs of store metrics retrieve the correct set of sources.
          if (this._storeIds.length === 0) {
            let nodeStatus: NodeStatus = nodeStatuses.GetStatus(this._nodeId);
            if (nodeStatus) {
              _.each(nodeStatus.store_statuses, (store: Models.Proto.StoreStatus) => {
                this._storeIds.push(String(store.desc.store_id));
              });
              this._initGraphs();
            }
          }
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
        let title: MithrilVirtualElement = m("", [
          m("a", {config: m.route, href: "/nodes"}, "Nodes"),
          ": Node " + ctrl.GetNodeId(),
        ]);

        // Primary content
        let primaryContent: MithrilElement;
        if (detail === "graph") {
          primaryContent = ctrl.RenderGraphs();
        } else {
          primaryContent = [
            ctrl.RenderPrimaryStats(),
            ctrl.RenderBuildInfo(),
          ];
        }

        let nodeStatus: NodeStatus = nodeStatuses.GetStatus(ctrl.GetNodeId());
        let updated: number = (nodeStatus ? nodeStatus.updated_at : 0);

        return m(".page", [
          m.component(Components.Topbar, {title: title, updated: updated}),
          m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          detail === "graph" ? m(".section.node", primaryContent) : primaryContent,
        ]);
      }
    }
  }
}
