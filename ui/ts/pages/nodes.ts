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
  import sumReducer = Models.Status.sumReducer;
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

      function AggregateByteColumn(keys: string[], section?: string, title?: string): Table.TableColumn<NodeStatus> {
        let byteSum: (s: NodeStatus) => number = (s: NodeStatus): number => {
          return _.sumBy(keys, (key: string) => {
            let byteKey: string = key + "_bytes";
            return parseFloat(_.get<string>(s, byteKey));
          });
        };

        return {
          title: title || Utils.Format.Titlecase(_.last(keys[0].split(".")) + "Bytes"),
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
              let lastUpdate: Moment = moment(Utils.Convert.NanoToMilli(status.stats.last_update_nanos));
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
            title: "Started",
            view: (status: NodeStatus): string => {
              return moment((Utils.Convert.NanoToMilli(status.started_at))).fromNow();
            },
            sortable: true,
          },
          AggregateByteColumn(["stats.live", "stats.intent", "stats.sys"], null, "Bytes"),
          {
            title: "Ranges",
            view: (status: NodeStatus): string => status.range_count.toString(),
            sortable: true,
            sortValue: (status: NodeStatus): number => status.range_count,
            rollup: function(rows: NodeStatus[]): string {
              return sumReducer("range_count", rows).toString();
            },
          },
          // TODO: add more stats
          {
            title: "Connections",
            view: (status: NodeStatus): string => "",
            sortable: true,
            sortValue: (status: NodeStatus): number => 0,
          },
          {
            title: "CPU Usage",
            view: (status: NodeStatus): string => "",
            sortable: true,
            sortValue: (status: NodeStatus): number => 0,
          },
          {
            title: "Mem Usage",
            view: (status: NodeStatus): string => "",
            sortable: true,
            sortValue: (status: NodeStatus): number => 0,
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
          return _.startsWith(m.route(), "/nodes" + (t.route ? "/" + t.route : "" ));
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

        let isOverview: boolean = _.startsWith(m.route(), "/nodes/overview");

        if (isOverview) {
          let comparisonData: Table.TableData<NodeStatus> = {
            columns: ctrl.columns,
            rows: nodeStatuses.allStatuses,
          };

          let mostRecentlyUpdated: number = _.max(_.map(nodeStatuses.allStatuses(), (s: NodeStatus) => s.updated_at));
          return m(".page", [
            m.component(Components.Topbar, {title: "Nodes", updated: mostRecentlyUpdated}),
            m.component(NavigationBar, {ts: ctrl.TargetSet()}),
            m(".section.table.node-overview", m(".stats-table", Components.Table.create(comparisonData))),
          ]);
        } else {
          return m(".page", [
            // TODO: add real updated time
            m.component(Components.Topbar, {title: "Nodes", updated: Utils.Convert.MilliToNano(Date.now()) }),
            m.component(NavigationBar, {ts: ctrl.TargetSet()}),
          ]);

        }
      }
    }

    /**
     * NodePage show the details of a single node.
     */
    export module NodePage {
      import Status = Models.Proto.Status;
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

          // ToStoreStatus converts a generic Status object to a StoreStatus object if possible and returns null otherwise
          function ToStoreStatus(s: Status): StoreStatus {
            if ((<StoreStatus>s).desc.store_id) {
              return (<StoreStatus>s);
            } else {
              return null;
            }
          }

          // CellInfo captures the human readable title and accessor function for a specific Store/Node status value
          interface CellInfo {
            title: string;
            valueFn: (s: Status) => string;
          }

          let values: CellInfo[] = [
            {title: "Started At", valueFn: (s: Status): string => Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(s.started_at)))},
            {title: "Updated At", valueFn: (s: Status): string => Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(s.updated_at)))},
            {title: "Live Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s.stats.live_bytes)},
            {title: "Key Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s.stats.key_bytes)},
            {title: "Value Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s.stats.val_bytes)},
            {title: "Intent Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s.stats.intent_bytes)},
            {title: "Sys Bytes", valueFn: (s: Status): string => Utils.Format.Bytes(s.stats.sys_bytes)},
            {title: "GC Bytes Age", valueFn: (s: Status): string => s.stats.gc_bytes_age.toString()},
            {title: "Total Ranges", valueFn: (s: Status): string => s.range_count.toString()},
            {title: "Leader Ranges", valueFn: (s: Status): string => s.leader_range_count.toString()},
            {title: "Available", valueFn: (s: Status): string => Utils.Format.Percentage(s.available_range_count, s.leader_range_count)},
            {title: "Fully Replicated", valueFn: (s: Status): string => Utils.Format.Percentage(s.replicated_range_count, s.leader_range_count)},

            // Store only values
            {title: "Available Capacity", valueFn: (s: Status): string => ToStoreStatus(s) ? Utils.Format.Bytes(ToStoreStatus(s).desc.capacity.available) : ""},
            {title: "Total Capacity", valueFn: (s: Status): string => ToStoreStatus(s) ? Utils.Format.Bytes(ToStoreStatus(s).desc.capacity.capacity) : ""},
          ];

          // genCells generates an array of virtual table cells populated with values from each node store
          // the only argument is the valueFn from CellInfo
          function genCells(valueFn: (s: StoreStatus) => string): MithrilVirtualElement[] {
            return _.map(nodeStats.store_ids, (id: number): MithrilVirtualElement => {
              let storeStatus: StoreStatus = storeStatuses.GetStatus(id.toString());
              return m("td.value", valueFn(storeStatus));
            });
          }

          // genRow generates the virtual table row from a CellInfo object
          function genRow(info: CellInfo): MithrilVirtualElement {
            return m("tr", [
              m("td.title", info.title),              // First cell: the human readable title for the row
              m("td.value", info.valueFn(nodeStats)), // Second cell: the node value
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
                  ].concat(_.map(nodeStats.store_ids, (id: number): MithrilVirtualElement => m("th.value", `Store ${id}`))))),
                m("tbody", _.map(values, genRow)),
              ])),
            ]);
          }
          return m(".primary-stats");
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

          // General activity stats.
          this._addChart(
            this.activityAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.conns"))
                .sources([this._nodeId])
                .title("Client Connections")
            ).format(d3.format("d")).title("SQL Connections")
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
            ).format(d3.format("d")).title("Queries Per Second")
          );

          // Add SQL charts.
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.select.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Selects")
            ).format(d3.format("d")).title("Reads")
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
            ).format(d3.format("d")).title("Writes")
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
            ).format(d3.format("d")).title("Transactions")
          );
          this._addChart(
            this.sqlAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("sql.ddl.count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("DDL Statements")
            ).format(d3.format("d")).title("Schema Changes")
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
                .title("Memory allocated")
            ).format(Utils.Format.Bytes).title("Memory Allocated")
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("goroutines"))
                .sources([this._nodeId])
                .title("goroutine count")
            ).format(d3.format("d")).title("goroutine Count")
          );
          this._addChart(
            this.systemAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_sysMetric("cgocalls"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("cgo Calls")
            ).format(d3.format("d")).title("cgo Calls")
          );

          // Graphs for internals, such as RocksDB
          this._addChart(
            this.internalsAxes,
            Metrics.NewAxis(
              Metrics.Select.Avg(_nodeMetric("txn.commits-count"))
                .sources([this._nodeId])
                .nonNegativeRate()
                .title("Commits"),
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
                .title("Block Cache Size")
            ).format(Utils.Format.Bytes).title("Block Cache Size")
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
            ).format(d3.format("d")).title("Block Cache Hits/Misses").stacked(true)
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
            ).format(d3.format("d")).title("Flushes and Compactions")
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
            ).format(d3.format("d")).title("Bloom Filter Prefix")
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
            ).format(d3.format(",d")).title("Garbage Collection")
            .label("Nanoseconds")
          );
        }

        private _refresh(): void {
          nodeStatuses.refresh();

          // When our NodeStatus has been retrieved, update our store_ids so
          // that graphs of store metrics retrieve the correct set of sources.
          if (this._storeIds.length === 0) {
            let nodeStatus: NodeStatus = nodeStatuses.GetStatus(this._nodeId);
            if (nodeStatus) {
              nodeStatus.store_ids.forEach((storeId: number) => {
                this._storeIds.push(String(storeId));
              });
              this._initGraphs();
            }
          }
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
        let title: MithrilVirtualElement = m("", [
          m("a", {config: m.route, href: "/nodes"}, "Nodes"),
          ": Node " + ctrl.GetNodeId(),
        ]);

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
          detail === "graph" ? m(".section.node", primaryContent) : primaryContent,
        ]);
      }
    }
  }
}
