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
import { shallow } from "enzyme";
import * as sinon from "sinon";
import classNames from "classnames/bind";
import styles from "./sortabletable.module.styl";

import "src/enzymeInit";
import {
  SortableTable,
  SortableColumn,
  SortSetting,
} from "src/views/shared/components/sortabletable";

const cx = classNames.bind(styles);

const columns: SortableColumn[] = [
  {
    title: "first",
    cell: (index) => index.toString() + ".first",
    sortKey: 1,
  },
  {
    title: "second",
    cell: (index) => index.toString() + ".second",
    sortKey: 2,
  },
  {
    title: "unsortable",
    cell: (index) => index.toString() + ".unsortable",
  },
];

function makeTable(
  count: number,
  sortSetting?: SortSetting,
  onChangeSortSetting?: (ss: SortSetting) => void,
) {
  return shallow(
    <SortableTable
      count={count}
      sortSetting={sortSetting}
      onChangeSortSetting={onChangeSortSetting}
      columns={columns}
    />,
  );
}

describe("<SortableTable>", () => {
  describe("renders correctly.", () => {
    it("renders the expected table structure.", () => {
      const wrapper = makeTable(1);
      assert.lengthOf(wrapper.find("table"), 1, "one table");
      assert.lengthOf(wrapper.find("thead").find("tr"), 1, "one header row");
      assert.lengthOf(
        wrapper.find(`tr.${cx("sort-table__row--header")}`),
        1,
        "column header row",
      );
      assert.lengthOf(wrapper.find("tbody"), 1, "tbody element");
    });

    it("renders rows and columns correctly.", () => {
      const rowCount = 5;
      const wrapper = makeTable(rowCount);

      // Verify header structure.
      assert.equal(
        wrapper.find("tbody").find("tr").length,
        rowCount,
        "correct number of rows",
      );
      const headers = wrapper.find(`tr.${cx("sort-table__row--header")}`);
      _.each(columns, (c, index) => {
        const header = headers.childAt(index);
        assert.isTrue(
          header.is(`.${cx("sort-table__cell")}`),
          "header is correct class.",
        );
        assert.equal(header.text(), c.title, "header has correct title.");
      });

      // Verify column contents.
      const rows = wrapper.find("tbody");
      _.times(rowCount, (rowIndex) => {
        const row = rows.childAt(rowIndex);
        assert.isTrue(row.is("tr"), "tbody contains rows");
        _.each(columns, (c, columnIndex) => {
          assert.equal(
            row.childAt(columnIndex).text(),
            c.cell(rowIndex),
            "table columns match",
          );
        });
      });

      // Nothing is sorted.
      assert.lengthOf(
        wrapper.find(`th.${cx("sort-table__cell--ascending")}`),
        0,
        "expected zero sorted columns.",
      );
      assert.lengthOf(
        wrapper.find(`th.${cx("sort-table__cell--descending")}`),
        0,
        "expected zero sorted columns.",
      );
    });

    it("renders sorted column correctly.", () => {
      // ascending = false.
      let wrapper = makeTable(1, { sortKey: 1, ascending: false });

      let sortHeader = wrapper.find(`th.${cx("sort-table__cell--descending")}`);
      assert.lengthOf(
        sortHeader,
        1,
        "only a single column is sorted descending.",
      );
      assert.equal(
        sortHeader.text(),
        columns[0].title,
        "first column should be sorted.",
      );
      sortHeader = wrapper.find(`th.${cx("sort-table__cell--ascending")}`);
      assert.lengthOf(sortHeader, 0, "no columns are sorted ascending.");

      // ascending = true
      wrapper = makeTable(1, { sortKey: 2, ascending: true });

      sortHeader = wrapper.find(`th.${cx("sort-table__cell--ascending")}`);
      assert.lengthOf(
        sortHeader,
        1,
        "only a single column is sorted ascending.",
      );
      assert.equal(
        sortHeader.text(),
        columns[1].title,
        "second column should be sorted.",
      );
      sortHeader = wrapper.find(`th.${cx("sort-table__cell--descending")}`);
      assert.lengthOf(sortHeader, 0, "no columns are sorted descending.");
    });
  });

  describe("changes sort setting on clicks.", () => {
    it("sorts descending on initial click.", () => {
      const spy = sinon.spy();
      const wrapper = makeTable(1, undefined, spy);
      wrapper
        .find(`th.${cx("sort-table__cell")}`)
        .first()
        .simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue(
        spy.calledWith({
          sortKey: 1,
          ascending: false,
        }),
      );
    });

    // Click on sorted data, different column.
    it("sorts descending on new column.", () => {
      const spy = sinon.spy();
      const wrapper = makeTable(1, { sortKey: 2, ascending: true }, spy);

      wrapper
        .find(`th.${cx("sort-table__cell")}`)
        .first()
        .simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue(
        spy.calledWith({
          sortKey: 1,
          ascending: false,
        }),
      );
    });

    it("sorts ascending if same column is clicked twice.", () => {
      const spy = sinon.spy();
      const wrapper = makeTable(1, { sortKey: 1, ascending: false }, spy);

      wrapper
        .find(`th.${cx("sort-table__cell")}`)
        .first()
        .simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue(
        spy.calledWith({
          sortKey: 1,
          ascending: true,
        }),
      );
    });

    it("removes sorting if same column is clicked thrice.", () => {
      const spy = sinon.spy();
      const wrapper = makeTable(1, { sortKey: 1, ascending: true }, spy);

      wrapper
        .find(`th.${cx("sort-table__cell")}`)
        .first()
        .simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue(
        spy.calledWith({
          sortKey: null,
          ascending: false,
        }),
      );
    });

    // Click on unsortable column does nothing.
    it("does nothing if unsortable column is clicked.", () => {
      const spy = sinon.spy();
      const wrapper = makeTable(1, { sortKey: 1, ascending: true }, spy);

      wrapper
        .find(`thead th.${cx("sort-table__cell")}`)
        .last()
        .simulate("click");
      assert.isTrue(spy.notCalled);
    });
  });
});
