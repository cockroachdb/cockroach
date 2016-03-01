// source: pages/sql.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
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
    import DatabaseList = Models.API.DatabaseList;
    import Database = Models.API.Database;
    import Grant = Models.API.Grant;
    import SQLColumn = Models.API.SQLColumn;
    import SQLIndex = Models.API.SQLIndex;
    import SQLTable = Models.API.SQLTable;

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
          rows: Utils.Computed(this.data, (data: DatabaseList): string[] => data.Databases),
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
          rows: Utils.Computed(this.data, (data: Database): string[] => data.Tables),
        });

        grantsTableData: Property<TableData<Grant>> = Prop({
          columns: Prop([
            {
              title: "User",
              view: (grant: Grant): string => grant.User,
            },
            {
              title: "Grants",
              view: (grant: Grant): string => grant.Privileges.join(", "),
            },
          ]),
          rows: Utils.Computed(this.data, (data: Database): Grant[] => data.Grants),
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
        indexColumns: string[] = ["Table", "Name", "Unique", "Seq", "Column", "Direction", "Storing"];
        columnColumns: string[] = ["Field", "Type", "Null", "Default"];
        displayJSON: boolean = false;
        data: Property<SQLTable> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        columnsTableData: Property<TableData<SQLColumn>> = Prop({
          columns: Prop(_.map(this.columnColumns, (k: string): TableColumn<SQLColumn> => {
            return {
              title: k,
              view: (column: SQLColumn): string => column[k],
            };
          })),
          rows: Utils.Computed(this.data, (data: SQLTable): SQLColumn[] => data.Columns),
        });

        indexTableData: Property<TableData<SQLIndex>> = Prop({
          columns: Prop(_.map(this.indexColumns, (k: string): TableColumn<SQLIndex> => {
            return {
              title: k,
              view: (column: SQLIndex): string => column[k],
            };
          })),
          rows: Utils.Computed(this.data, (data: SQLTable): SQLIndex[] => data.Index),
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
            m(
              ".sql-table", [
                m("h1", "Columns"),
                Components.Table.create(ctrl.columnsTableData()),
                m("h1", "Indices"),
                Components.Table.create(ctrl.indexTableData()),
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
