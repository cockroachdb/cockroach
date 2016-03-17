// source: pages/sql.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../models/api.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module AdminViews {
  "use strict";
  export module Databases {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;
    import TableData = Components.Table.TableData;
    import TableColumn = Components.Table.TableColumn;
    import MithrilController = _mithril.MithrilController;
    import Property = Utils.Property;
    import Prop = Utils.Prop;
    import DatabaseList = Models.Proto.DatabaseList;
    import Database = Models.Proto.Database;
    import Grant = Models.Proto.Grant;
    import SQLColumn = Models.Proto.SQLColumn;
    import SQLIndex = Models.Proto.SQLIndex;
    import SQLTable = Models.Proto.SQLTable;

    // Page listing all the databases
    export module DatabaseListPage {
      class DBListController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<DatabaseList> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        tableData: Property<TableData<string>> = Prop({
          columns: Prop([
            {
              title: "Database",
              view: function (db: string): MithrilVirtualElement {
                return m("a", {config: m.route, href: `/databases/${db}`}, db);
              },
            },
          ]),
          rows: Utils.Computed(this.data, (data: DatabaseList): string[] => data.databases),
        });

        toggleDisplayJSON(): void {
          this.displayJSON = !this.displayJSON;
        };
        toggleText(): string {
          return this.displayJSON ? "Hide JSON Response" : "Show JSON Response";
        };
      }

      export function controller(): DBListController {
        let ctrl: DBListController = new DBListController();
        Models.API.databases().then(ctrl.data).then((): void => { ctrl.updated(Date.now()); });
        return ctrl;
      }

      export function view(ctrl: DBListController): MithrilVirtualElement {
        return m(".page", [
          m.component(Components.Topbar, {title: "Databases", updated: Utils.Convert.MilliToNano(ctrl.updated()) }),
          m(".section",
            m(
              ".sql-table", [
                Components.Table.create(ctrl.tableData()),
                m(
                  "a.toggle",
                  {onclick: function(): void { ctrl.toggleDisplayJSON(); } },
                  ctrl.toggleText()
                ),
                m(
                  "pre",
                  {style: {display: ctrl.displayJSON ? null : "none"}},
                  JSON.stringify(ctrl.data(), null, 2)
                ),
              ]
            )
          ),
        ]);
      }
    }

    // Page listing the tables/grants for a specific database
    export module DatabasePage {
      class DBController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<Database> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        tablesTableData: Property<TableData<string>> = Prop({
          columns: Prop([
            {
              title: "Table",
              view: (table: string): MithrilVirtualElement =>
                m("a", {config: m.route, href: `/databases/${m.route.param("database")}/tables/${table}`}, table),
            },
          ]),
          rows: Utils.Computed(this.data, (data: Database): string[] => data.table_names),
        });

        grantsTableData: Property<TableData<Grant>> = Prop({
          columns: Prop([
            {
              title: "User",
              view: (grant: Grant): string => grant.user,
            },
            {
              title: "Grants",
              view: (grant: Grant): string => grant.privileges.join(", "),
            },
          ]),
          rows: Utils.Computed(this.data, (data: Database): Grant[] => data.grants),
        });

        toggleDisplayJSON(): void {
          this.displayJSON = !this.displayJSON;
        };
        toggleText(): string {
          return this.displayJSON ? "Hide JSON Response" : "Show JSON Response";
        };
      }

      export function controller(): DBController {
        let ctrl: DBController = new DBController();
        Models.API.database(m.route.param("database")).then(ctrl.data).then((): void => { ctrl.updated(Date.now()); });
        return ctrl;
      }

      export function view(ctrl: DBController): MithrilVirtualElement {
        return m(".page", [
          m.component(Components.Topbar, {
            title: m("", [m("a", {config: m.route, href: "/databases"}, "Databases"), `: ${m.route.param("database")}`]),
            updated: Utils.Convert.MilliToNano(ctrl.updated()),
          }),
          m(".section",
            m(
              ".sql-table", [
                m("h1", "Tables"),
                Components.Table.create(ctrl.tablesTableData()),
                m("h1", "Grants"),
                Components.Table.create(ctrl.grantsTableData()),
                m(
                  "a.toggle",
                  {onclick: function(): void { ctrl.toggleDisplayJSON(); } },
                  ctrl.toggleText()
                ),
                m(
                  "pre",
                  {style: {display: ctrl.displayJSON ? null : "none"}},
                  JSON.stringify(ctrl.data(), null, 2)
                ),
              ]
            )
          ),
        ]);
      }
    }

    // Page listing the columns/indices for a specific table
    export module TablePage {
      class TableController implements MithrilController {
        indexColumns: string[] = ["name", "unique", "seq", "column", "direction", "storing"];
        columnColumns: string[] = ["name", "type", "nullable", "default"];
        displayJSON: boolean = false;
        data: Property<SQLTable> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        columnsTableData: Property<TableData<SQLColumn>> = Prop({
          columns: Prop(_.map<string, TableColumn<SQLColumn>>(this.columnColumns, (k: string): TableColumn<SQLColumn> => {
            return {
              title: k,
              view: (column: SQLColumn): string => column[k],
            };
          })),
          rows: Utils.Computed(this.data, (data: SQLTable): SQLColumn[] => data.columns),
        });

        indexTableData: Property<TableData<SQLIndex>> = Prop({
          columns: Prop(_.map<string, TableColumn<SQLIndex>>(this.indexColumns, (k: string): TableColumn<SQLIndex> => {
            return {
              title: k,
              view: (column: SQLIndex): string => column[k],
            };
          })),
          rows: Utils.Computed(this.data, (data: SQLTable): SQLIndex[] => data.indexes),
        });

        grantsTableData: Property<TableData<Grant>> = Prop({
          columns: Prop([
            {
              title: "User",
              view: (grant: Grant): string => grant.user,
            },
            {
              title: "Grants",
              view: (grant: Grant): string => grant.privileges.join(", "),
            },
          ]),
          rows: Utils.Computed(this.data, (data: SQLTable): Grant[] => data.grants),
        });

        toggleDisplayJSON(): void {
          this.displayJSON = !this.displayJSON;
        };
        toggleText(): string {
          return this.displayJSON ? "Hide JSON Response" : "Show JSON Response";
        };
      }

      export function controller(): TableController {
        let ctrl: TableController = new TableController();
        Models.API.table(m.route.param("database"), m.route.param("table")).then(ctrl.data).then((): void => { ctrl.updated(Date.now()); });
        return ctrl;
      }

      export function view(ctrl: TableController): MithrilVirtualElement {
        return m(".page", [
          m.component(Components.Topbar, {
            title: m("", [
              m("a", {config: m.route, href: "/databases"}, "Databases"),
              ": " ,
              m("a", {config: m.route, href: `/databases/${m.route.param("database")}`}, m.route.param("database")),
              `: ${m.route.param("table")}`,
            ]),
            updated: Utils.Convert.MilliToNano(ctrl.updated()),
          }),
          m(".section",
            m("h1", "Number of ranges"),
            m(".num-ranges", ctrl.data().range_count),
            m(
              ".sql-table", [
                m("h1", "Columns"),
                Components.Table.create(ctrl.columnsTableData()),
                m("h1", "Indices"),
                Components.Table.create(ctrl.indexTableData()),
                m("h1", "Grants"),
                Components.Table.create(ctrl.grantsTableData()),
                m(
                  "a.toggle",
                  {onclick: function(): void { ctrl.toggleDisplayJSON(); } },
                  ctrl.toggleText()
                ),
                m(
                  "pre",
                  {style: {display: ctrl.displayJSON ? null : "none"}},
                  JSON.stringify(ctrl.data(), null, 2)
                ),
              ]
            )
          ),
        ]);
      }
    }
  }
}
