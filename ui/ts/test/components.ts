/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../components/table.ts" />
/// <reference path="../util/property.ts" />

module TestData {
  "use strict";
  export interface TestTableRow {
    id: number;
    title: string;
    value: number;
  }
}

module TestTable {
  "use strict";
  import Table = Components.Table;
  import TestTableRow = TestData.TestTableRow;
  import MithrilElement = _mithril.MithrilVirtualElement;

  suite("Table Component", () => {
    let columns: Table.TableColumn<TestTableRow>[] = [
      {
        title: "ID Column",
        view: (r: TestTableRow): MithrilElement => m("span.id", r.id),
        sortable: true,
        sortValue: (r: TestTableRow): number => r.id,
      },
      {
        title: "Title",
        view: (r: TestTableRow): string => r.title,
        sortable: false,
      },
      {
        title: "Value",
        view: (r: TestTableRow): MithrilElement => m("span.value", r.value),
        sortable: true,
        sortValue: (r: TestTableRow): number => r.value,
      },
    ];
    let data: Table.TableData<TestTableRow> = {
      columns: Utils.Prop(columns),
      rows: Utils.Prop([
        {
          id: 2,
          title: "CCC",
          value: 60,
        },
        {
          id: 1,
          title: "AAA",
          value: 40,
        },
        {
          id: 3,
          title: "BBB",
          value: 10,
        },
      ]),
    };

    test("Sorts data correctly", () => {
      let table = Table.controller(data);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [2, 1, 3]);
      // Sort by ID.
      table.SetSortColumn(columns[0]);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [1, 2, 3]);
      // Reverse.
      table.SetSortColumn(columns[0]);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [3, 2, 1]);

      // Sort by value.
      table.SetSortColumn(columns[2]);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [3, 1, 2]);
      // Reverse.
      table.SetSortColumn(columns[2]);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [2, 1, 3]);

      // Non-sortable column restores original order.
      table.SetSortColumn(columns[1]);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [2, 1, 3]);
      // Reverse does nothing.
      table.SetSortColumn(columns[1]);
      chai.assert.deepEqual(_.map(table.sortedRows(), "id"), [2, 1, 3]);
    });
  });
}
