// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import _ from "lodash";
import { assert } from "chai";
import { mount, ReactWrapper } from "enzyme";
import * as sinon from "sinon";
import classNames from "classnames/bind";
import {
  SortedTable,
  ColumnDescriptor,
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable";
import styles from "src/sortabletable/sortabletable.module.scss";

const cx = classNames.bind(styles);

class TestRow {
  constructor(public name: string, public value: number) {}
}

const columns: ColumnDescriptor<TestRow>[] = [
  {
    name: "first",
    title: "first",
    cell: tr => tr.name,
    sort: tr => tr.name,
  },
  {
    name: "second",
    title: "second",
    cell: tr => tr.value.toString(),
    sort: tr => tr.value,
    rollup: trs => _.sumBy(trs, tr => tr.value),
  },
];

class TestSortedTable extends SortedTable<TestRow> {}

function makeTable(
  data: TestRow[],
  sortSetting?: SortSetting,
  onChangeSortSetting?: (ss: SortSetting) => void,
  pagination?: ISortedTablePagination,
) {
  return mount(
    <TestSortedTable
      data={data}
      sortSetting={sortSetting}
      onChangeSortSetting={onChangeSortSetting}
      pagination={pagination}
      columns={columns}
    />,
  );
}

function makeExpandableTable(data: TestRow[], sortSetting: SortSetting) {
  return mount(
    <TestSortedTable
      data={data}
      columns={columns}
      sortSetting={sortSetting}
      expandableConfig={{
        expandedContent: testRow => (
          <div>
            {testRow.name}={testRow.value}
          </div>
        ),
        expansionKey: testRow => testRow.name,
      }}
    />,
  );
}

function rowsOf(wrapper: ReactWrapper): Array<Array<string>> {
  return wrapper.find("tr").map(tr => tr.find("td").map(td => td.text()));
}

describe("<SortedTable>", function() {
  it("renders the expected table structure.", function() {
    const wrapper = makeTable([new TestRow("test", 1)]);
    assert.lengthOf(wrapper.find("table"), 1, "one table");
    assert.lengthOf(wrapper.find("thead").find("tr"), 1, "one header row");
    assert.lengthOf(
      wrapper.find(`tr.${cx("head-wrapper__row--header")}`),
      1,
      "column header row",
    );
    assert.lengthOf(wrapper.find("tbody"), 1, "tbody element");
  });

  it("correctly uses onChangeSortSetting", function() {
    const spy = sinon.spy();
    const wrapper = makeTable([new TestRow("test", 1)], undefined, spy);
    wrapper
      .find(`th.${cx("head-wrapper__cell")}`)
      .first()
      .simulate("click");
    assert.isTrue(spy.calledOnce);
    assert.deepEqual(spy.getCall(0).args[0], {
      ascending: false,
      columnTitle: "first",
    } as SortSetting);
  });

  it("correctly sorts data based on sortSetting", function() {
    const data = [
      new TestRow("c", 3),
      new TestRow("d", 4),
      new TestRow("a", 1),
      new TestRow("b", 2),
    ];
    let wrapper = makeTable(data, undefined);
    const assertMatches = (expected: TestRow[]) => {
      const rows = wrapper.find("tbody");
      _.each(expected, (rowData, dataIndex) => {
        const row = rows.childAt(dataIndex);
        assert.equal(
          row
            .childAt(0)
            .childAt(0)
            .text(),
          rowData.name,
          "first columns match",
        );
        assert.equal(
          row
            .childAt(0)
            .childAt(1)
            .text(),
          rowData.value.toString(),
          "second columns match",
        );
      });
    };
    assertMatches(data);
    wrapper = makeTable(data, {
      ascending: true,
      columnTitle: "first",
    });
    assertMatches(_.sortBy(data, r => r.name));
    wrapper.setProps({
      uiSortSetting: {
        ascending: true,
        columnTitle: "second",
      } as SortSetting,
    });
    assertMatches(_.sortBy(data, r => r.value));
  });

  describe("with expandableConfig", function() {
    it("renders the expected table structure", function() {
      const wrapper = makeExpandableTable([new TestRow("test", 1)], undefined);
      assert.lengthOf(wrapper.find("table"), 1, "one table");
      assert.lengthOf(wrapper.find("thead").find("tr"), 1, "one header row");
      assert.lengthOf(
        wrapper.find(`tr.${cx("head-wrapper__row--header")}`),
        1,
        "column header row",
      );
      assert.lengthOf(wrapper.find("tbody"), 1, "tbody element");
      assert.lengthOf(wrapper.find("tbody tr"), 1, "one body row");
      assert.lengthOf(
        wrapper.find("tbody td"),
        3,
        "two body cells plus one expansion control cell",
      );
      assert.lengthOf(
        wrapper.find(`td.${cx("row-wrapper__cell__expansion-control")}`),
        1,
        "one expansion control cell",
      );
    });

    it("expands and collapses the clicked row", function() {
      const wrapper = makeExpandableTable([new TestRow("test", 1)], undefined);
      assert.lengthOf(
        wrapper.find(`.${cx("row-wrapper__row--expanded-area")}`),
        0,
        "nothing expanded yet",
      );
      wrapper
        .find(`.${cx("row-wrapper__cell__expansion-control")}`)
        .simulate("click");
      const expandedArea = wrapper.find(".row-wrapper__row--expanded-area");
      assert.lengthOf(expandedArea, 1, "row is expanded");
      assert.lengthOf(
        expandedArea.children(),
        2,
        "expanded row contains placeholder and content area",
      );
      assert.isTrue(expandedArea.contains(<td />));
      assert.isTrue(
        expandedArea.contains(
          <td className={cx("row-wrapper__cell")} colSpan={2}>
            <div>test=1</div>
          </td>,
        ),
      );
      wrapper
        .find(`.${cx("row-wrapper__cell__expansion-control")}`)
        .simulate("click");
      assert.lengthOf(
        wrapper.find(`.${cx("row-wrapper__row--expanded-area")}`),
        0,
        "row collapsed again",
      );
    });
  });

  it("should correctly render rows with pagination and sort settings", function() {
    const data = [
      new TestRow("c", 3),
      new TestRow("d", 4),
      new TestRow("a", 1),
      new TestRow("b", 2),
    ];
    let wrapper = makeTable(data, undefined, undefined, {
      current: 1,
      pageSize: 2,
    });
    let rows = wrapper.find("tbody");
    assert.lengthOf(wrapper.find("tbody tr"), 2, "two body rows");
    assert.equal(
      rows
        .childAt(1)
        .childAt(0)
        .childAt(0)
        .text(),
      "d",
      "second row column at first page match",
    );

    wrapper = makeTable(data, undefined, undefined, {
      current: 2,
      pageSize: 2,
    });
    rows = wrapper.find("tbody");
    assert.lengthOf(wrapper.find("tbody tr"), 2, "two body rows");
    assert.equal(
      rows
        .childAt(0)
        .childAt(0)
        .childAt(0)
        .text(),
      "a",
      "first row column at seconds page match",
    );

    wrapper = makeTable(
      data,
      { ascending: true, columnTitle: "first" },
      undefined,
      {
        current: 1,
        pageSize: 2,
      },
    );
    rows = wrapper.find("tbody");
    assert.equal(
      rows
        .childAt(1)
        .childAt(0)
        .childAt(0)
        .text(),
      "b",
      "second row column at first page match",
    );

    wrapper = makeTable(
      data,
      { ascending: true, columnTitle: "first" },
      undefined,
      {
        current: 2,
        pageSize: 2,
      },
    );
    rows = wrapper.find("tbody");
    assert.equal(
      rows
        .childAt(0)
        .childAt(0)
        .childAt(0)
        .text(),
      "c",
      "first row column at seconds page match",
    );
  });

  it("should update when pagination changes", function() {
    const table = makeTable(
      [
        new TestRow("c", 3),
        new TestRow("d", 4),
        new TestRow("a", 1),
        new TestRow("b", 2),
      ],
      undefined,
      undefined,
      {
        current: 1,
        pageSize: 2,
      },
    );

    assert.deepEqual(rowsOf(table.find("tbody")), [
      ["c", "3"],
      ["d", "4"],
    ]);

    table.setProps({ pagination: { current: 2, pageSize: 2 } });

    assert.deepEqual(rowsOf(table.find("tbody")), [
      ["a", "1"],
      ["b", "2"],
    ]);
  });
});
