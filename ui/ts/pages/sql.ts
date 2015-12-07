// source: pages/sql.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/moment/moment.d.ts" />
/// <reference path="../../typings/moment-timezone/moment-timezone.d.ts"/>
/// <reference path="../models/sqlquery.ts" />
/// <reference path="../models/proto.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module AdminViews {
  "use strict";
  export module SQL {
    export module Page {
      import MithrilVirtualElement = _mithril.MithrilVirtualElement;
      import MithrilBasicProperty = _mithril.MithrilBasicProperty;
      import MithrilView = _mithril.MithrilView;
      import TableData = Components.Table.TableData;
      import Column = Models.Proto.Column;
      import Row = Models.Proto.Row;
      import Response = Models.Proto.Response;
      import TableColumn = Components.Table.TableColumn;
      import MithrilController = _mithril.MithrilController;
      import Datum = Models.Proto.Datum;

      let data: MithrilBasicProperty<Response> = m.prop({});
      let columns: Utils.Property<TableColumn<Row>[]> = Utils.Prop([]);
      let rows: Utils.Property<Row[]> = Utils.Prop([]);

      let tableData: TableData<Row> = {
        columns: columns,
        rows: rows,
      };
      let query: MithrilBasicProperty<string> = m.prop("");

      function runQuery(q: string): void {
        Models.SQLQuery.runQuery(q).then(data);
      }

      function populateTableDataFromResult(result: Response, table: TableData<Row>): void {
        let cols: Column[] = _.get(result, "results[0].Union.Rows.columns", []);
        let resultRows: Row[] = _.get(result, "results[0].Union.Rows.rows", []);

        (<Utils.Property<TableColumn<Row>[]>>table.columns)(_.map<Column, TableColumn<Row>>(cols, function (col: Column, i: number): TableColumn<Row> {
            return {
              title: col.name,
              view: function (row: Row): string {
              let type: string = _.keys(col.typ.Payload)[0];
              let payload: Datum = row.values[i].Payload;
              let viewVal: string = "";
              if (type === "BytesVal") {
                viewVal = payload.BytesVal.toString();
              } else if (type === "TimeVal") {
                viewVal = Utils.Convert.TimestampToMoment(payload.TimeVal).toString();
              } else if (type === "DateVal") {
                viewVal = moment.utc(0).add(payload.DateVal, "days").format("YYYY-MM-DD");
              } else {
                viewVal = payload[type].toString();
              }
              return viewVal;
              },
            };
          }));

        (<Utils.Property<Row[]>>table.rows)(resultRows);
      }

      class DisplayToggler implements MithrilController {
        displayJSON: boolean;

        constructor() {
          this.displayJSON = false;
        }

        toggleDisplayJSON(): void {
          this.displayJSON = !this.displayJSON;
        };
        toggleText(): string {
          return this.displayJSON ? "Hide JSON Response" : "Show JSON Response";
        };
      }

      export function controller(): DisplayToggler {
        query(m.route.param("q"));
        runQuery(query());
        return new DisplayToggler();
      }

      export let view: MithrilView<DisplayToggler> = function (ctrl: DisplayToggler): MithrilVirtualElement {

        populateTableDataFromResult(data(), tableData);

        return m(".page", [
          m.component(Components.Topbar, {title: m.route.param("title")}),
          m(".section",
            m(
              ".sql-table", [
                Components.Table.create(tableData),
                m("a.toggle", {onclick: function(): void { ctrl.toggleDisplayJSON(); } }, ctrl.toggleText()),
                m("pre", {style: {display: ctrl.displayJSON ? null : "none"}}, JSON.stringify(data(), null, 2)),
              ])),
        ]);
      };
    }
  }
}
