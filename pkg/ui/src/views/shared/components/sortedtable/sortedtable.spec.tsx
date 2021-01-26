// Copyright 2018 The Cockroach Authors.
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
import { mount } from "enzyme";
import * as sinon from "sinon";
import classNames from "classnames/bind";

import "src/enzymeInit";
import {
  SortedTable,
  ColumnDescriptor,
  ISortedTablePagination,
} from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import styles from "../sortabletable/sortabletable.module.styl";

const cx = classNames.bind(styles);

class TestRow {
  constructor(public name: string, public value: number) {}
}

const columns: ColumnDescriptor<TestRow>[] = [
  {
    title: "first",
    cell: (tr) => tr.name,
    sort: (tr) => tr.name,
  },
  {
    title: "second",
    cell: (tr) => tr.value.toString(),
    sort: (tr) => tr.value,
    rollup: (trs) => _.sumBy(trs, (tr) => tr.value),
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
        expandedContent: (testRow) => (
          <div>
            {testRow.name}={testRow.value}
          </div>
        ),
        expansionKey: (testRow) => testRow.name,
      }}
    />,
  );
}

describe("<SortedTable>", function () {
  it("renders the expected table structure.", function () {
    const wrapper = makeTable([new TestRow("test", 1)]);
    assert.lengthOf(wrapper.find("table"), 1, "one table");
    assert.lengthOf(wrapper.find("thead").find("tr"), 1, "one header row");
    assert.lengthOf(
      wrapper.find(`tr.${cx("sort-table__row--header")}`),
      1,
      "column header row",
    );
    assert.lengthOf(wrapper.find("tbody"), 1, "tbody element");
  });

  it("correctly uses onChangeSortSetting", function () {
    const spy = sinon.spy();
    const wrapper = makeTable([new TestRow("test", 1)], undefined, spy);
    wrapper
      .find(`th.${cx("sort-table__cell")}`)
      .first()
      .simulate("click");
    assert.isTrue(spy.calledOnce);
    assert.deepEqual(spy.getCall(0).args[0], {
      sortKey: 0,
      ascending: false,
    } as SortSetting);
  });

  it("correctly sorts data based on sortSetting", function () {
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
          row.childAt(0).text(),
          rowData.name,
          "first columns match",
        );
        assert.equal(
          row.childAt(1).text(),
          rowData.value.toString(),
          "second columns match",
        );
      });
    };
    assertMatches(data);
    wrapper = makeTable(data, { sortKey: 0, ascending: true });
    assertMatches(_.sortBy(data, (r) => r.name));
    wrapper.setProps({
      uiSortSetting: { sortKey: 1, ascending: true } as SortSetting,
    });
    assertMatches(_.sortBy(data, (r) => r.value));
  });

  describe("with expandableConfig", function () {
    it("renders the expected table structure", function () {
      const wrapper = makeExpandableTable([new TestRow("test", 1)], undefined);
      assert.lengthOf(wrapper.find("table"), 1, "one table");
      assert.lengthOf(wrapper.find("thead").find("tr"), 1, "one header row");
      assert.lengthOf(
        wrapper.find(`tr.${cx("sort-table__row--header")}`),
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
        wrapper.find(`td.${cx("sort-table__cell__expansion-control")}`),
        1,
        "one expansion control cell",
      );
    });

    it("expands and collapses the clicked row", function () {
      const wrapper = makeExpandableTable([new TestRow("test", 1)], undefined);
      assert.lengthOf(
        wrapper.find(`.${cx("sort-table__row--expanded-area")}`),
        0,
        "nothing expanded yet",
      );
      wrapper
        .find(`.${cx("sort-table__cell__expansion-control")}`)
        .simulate("click");
      const expandedArea = wrapper.find(".sort-table__row--expanded-area");
      assert.lengthOf(expandedArea, 1, "row is expanded");
      assert.lengthOf(
        expandedArea.children(),
        2,
        "expanded row contains placeholder and content area",
      );
      assert.isTrue(expandedArea.contains(<td />));
      assert.isTrue(
        expandedArea.contains(
          <td className={cx("sort-table__cell")} colSpan={2}>
            <div>test=1</div>
          </td>,
        ),
      );
      wrapper
        .find(`.${cx("sort-table__cell__expansion-control")}`)
        .simulate("click");
      assert.lengthOf(
        wrapper.find(`.${cx("sort-table__row--expanded-area")}`),
        0,
        "row collapsed again",
      );
    });
  });

  it("should correctly render rows with pagination and sort settings", function () {
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
      rows.childAt(1).childAt(0).text(),
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
      rows.childAt(0).childAt(0).text(),
      "a",
      "first row column at seconds page match",
    );

    wrapper = makeTable(data, { sortKey: 0, ascending: true }, undefined, {
      current: 1,
      pageSize: 2,
    });
    rows = wrapper.find("tbody");
    assert.equal(
      rows.childAt(1).childAt(0).text(),
      "b",
      "second row column at first page match",
    );

    wrapper = makeTable(data, { sortKey: 0, ascending: true }, undefined, {
      current: 2,
      pageSize: 2,
    });
    rows = wrapper.find("tbody");
    assert.equal(
      rows.childAt(0).childAt(0).text(),
      "c",
      "first row column at seconds page match",
    );
  });
});
