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
    import ReadOnlyProperty = Utils.ReadOnlyProperty;
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
      function populateDatabaseListTable(databaseList: DatabaseList): TableData<string> {
        let columns: Property<TableColumn<string>[]> = Prop([]);
        let rows: Property<string[]> = Prop([]);
        columns([
          {
            title: "Database",
            view: function (db: string): MithrilVirtualElement {
              return m("a", {config: m.route, href: `/databases/${db}`}, db);
            },
          },
        ]);

        rows(databaseList.Databases);

        return {
          columns: columns,
          rows: rows,
        };
      }

      class DBListController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<DatabaseList> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        tableData: ReadOnlyProperty<TableData<string>> = Utils.Computed(this.data, populateDatabaseListTable);

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
      function populateTablesTable(database: Database): TableData<string> {
        let columns: Property<TableColumn<string>[]> = Prop([]);
        let rows: Property<string[]> = Prop([]);
        columns([
          {
            title: "Table",
            view: function (table: string): MithrilVirtualElement {
              return m("a", {config: m.route, href: `/databases/${m.route.param("database")}/tables/${table}`}, table);
            },
          },
        ]);

        rows(database.Tables);

        return {
          columns: columns,
          rows: rows,
        };
      }

      function populateGrantsTable(database: Database): TableData<Grant> {
        let columns: Property<TableColumn<Grant>[]> = Prop([]);
        let rows: Property<Grant[]> = Prop([]);
        columns([
          {
            title: "User",
            view: function (grant: Grant): string {
              return grant.User;
            },
          },
          {
            title: "Grants",
            view: function (grant: Grant): string {
              return grant.Privileges.join(", ");
            },
          },
        ]);

        rows(database.Grants);

        return {
          columns: columns,
          rows: rows,
        };
      }

      class DBController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<Database> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        tablesTableData: ReadOnlyProperty<TableData<string>> = Utils.Computed(this.data, populateTablesTable);
        grantsTableData: ReadOnlyProperty<TableData<Grant>> = Utils.Computed(this.data, populateGrantsTable);

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
      function populateTableColumnData(table: SQLTable): TableData<SQLColumn> {
        let columns: Property<TableColumn<SQLColumn>[]> = Prop([]);
        let rows: Property<SQLColumn[]> = Prop([]);
        columns(_.map(["Field", "Type", "Null", "Default"], (k: string): TableColumn<SQLColumn> => {
          return {
            title: k,
            view: (column: SQLColumn): string => {
              return column[k];
            },
          };
        }));

        rows(table.Columns);

        return {
          columns: columns,
          rows: rows,
        };
      }

      function populateTableIndexData(table: SQLTable): TableData<SQLIndex> {
        let columns: Property<TableColumn<SQLIndex>[]> = Prop([]);
        let rows: Property<SQLIndex[]> = Prop([]);
        columns(_.map(["Table", "Name", "Unique", "Seq", "Column", "Direction", "Storing"], (k: string): TableColumn<SQLIndex> => {
          return {
            title: k,
            view: (index: SQLIndex): string => {
              return index[k];
            },
          };
        }));

        rows(table.Index);

        return {
          columns: columns,
          rows: rows,
        };
      }

      class TableController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<SQLTable> = Prop(null);
        updated: Property<number> = Prop(Date.now());
        tableColumnData: ReadOnlyProperty<TableData<SQLColumn>> = Utils.Computed(this.data, populateTableColumnData);
        tableIndexData: ReadOnlyProperty<TableData<SQLIndex>> = Utils.Computed(this.data, populateTableIndexData);

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
                Components.Table.create(ctrl.tableColumnData()),
                m("h1", "Indices"),
                Components.Table.create(ctrl.tableIndexData()),
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
