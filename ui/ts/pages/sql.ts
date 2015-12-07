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
      import TableData = Components.Table.TableData;
      import Column = Models.Proto.Column;
      import Row = Models.Proto.Row;
      import Response = Models.Proto.Response;
      import TableColumn = Components.Table.TableColumn;
      import MithrilController = _mithril.MithrilController;
      import Datum = Models.Proto.Datum;
      import ReadOnlyProperty = Utils.ReadOnlyProperty;
      import Property = Utils.Property;
      import Prop = Utils.Prop;

      function runQuery(q: string, data: Property<Response>, updated: Property<number>): void {
        Models.SQLQuery.runQuery(q).then(data).then(function(): void {
          updated(Date.now());
        });
      }

      function populateTableDataFromResult(result: Response): TableData<Row> {
        let cols: Column[] = _.get(result, "results[0].Union.Rows.columns", []);
        let resultRows: Row[] = _.get(result, "results[0].Union.Rows.rows", []);
        let columns: Property<TableColumn<Row>[]> = Prop([]);
        let rows: Property<Row[]> = Prop([]);
        columns(_.map(cols, function (col: Column, i: number): TableColumn<Row> {
            return {
              title: col.name,
              view: function (row: Row): string {
                let payload: Datum = row.values[i].Payload;
                let type: string = _.keys(payload)[0];
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

        rows(resultRows);

        return {
          columns: columns,
          rows: rows,
        };
      }

      class SQLController implements MithrilController {
        displayJSON: boolean = false;
        data: Property<Response> = Prop({});
        updated: Property<number> = Prop(Date.now());
        tableData: ReadOnlyProperty<TableData<Row>> = Utils.Computed(this.data, function(response: Response): TableData<Row> {
          return populateTableDataFromResult(response);
        });
        query: MithrilBasicProperty<string> = m.prop("");

        toggleDisplayJSON(): void {
          this.displayJSON = !this.displayJSON;
        };
        toggleText(): string {
          return this.displayJSON ? "Hide JSON Response" : "Show JSON Response";
        };
      }

      export function controller(): SQLController {
        let ctrl: SQLController = new SQLController();
        ctrl.query(m.route.param("q"));
        runQuery(ctrl.query(), ctrl.data, ctrl.updated);
        return ctrl;
      }

      export function view(ctrl: SQLController): MithrilVirtualElement {
        return m(".page", [
          m.component(Components.Topbar, {title: m.route.param("title"), updated: Utils.Convert.MilliToNano(ctrl.updated()) }),
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
      };
    }
  }
}
