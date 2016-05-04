import * as React from "react";
import _ = require("lodash");
import { assert } from "chai";
import { shallow } from "enzyme";
import * as sinon from "sinon";

import { SortableTable, SortableColumn, SortSetting } from "./sortabletable";

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

function makeTable(count: number, sortSetting?: SortSetting,
                   onChangeSortSetting?: (ss: SortSetting) => void) {
  return shallow(<SortableTable count={count}
                                sortSetting={sortSetting}
                                onChangeSortSetting={onChangeSortSetting}>
                    { columns }
                 </SortableTable>);
}

describe("<SortableTable>", () => {
  describe("renders correctly.", () => {
    it("renders the expected table structure.", () => {
      let wrapper = makeTable(1);
      assert.lengthOf(wrapper.find("table"), 1, "one table");
      assert.lengthOf(wrapper.find("thead").find("tr"), 2, "two header rows");
      assert.lengthOf(wrapper.find("tr.column"), 1, "column header row");
      assert.lengthOf(wrapper.find("tr.rollup"), 1, "rollup header row");
      assert.lengthOf(wrapper.find("tbody"), 1, "tbody element");
    });

    it("renders rows and columns correctly.", () => {
      const rowCount = 5;
      let wrapper = makeTable(rowCount);

      // Verify header structure.
      assert.equal(wrapper.find("tbody").find("tr").length, rowCount, "correct number of rows");
      let headers = wrapper.find("tr.column");
      _.each(columns, (c, index) => {
        let header = headers.childAt(index);
        assert.isTrue(header.is(".column"), "header is correct class.");
        assert.equal(header.text(), c.title, "header has correct title.");
      });

      // Verify column contents.
      let rows = wrapper.find("tbody");
      _.times(rowCount, (rowIndex) => {
        let row = rows.childAt(rowIndex);
        assert.isTrue(row.is("tr"), "tbody contains rows");
        _.each(columns, (c, columnIndex) => {
          assert.equal(row.childAt(columnIndex).text(), c.cell(rowIndex), "table columns match");
        });
      });

      // Nothing is sorted.
      assert.lengthOf(wrapper.find("th.sorted"), 0, "expected zero sorted columns.");
    });

    it("renders sorted column correctly.", () => {
      // ascending = false.
      let wrapper = makeTable(1, { sortKey: 1, ascending: false });

      let sortHeader = wrapper.find("th.sorted");
      assert.lengthOf(sortHeader, 1, "only a single column is sorted");
      assert.isFalse(sortHeader.is(".ascending"), "sorted column should not be ascending");
      assert.equal(sortHeader.text(), columns[0].title, "first column should be sorted.");

      // ascending = true
      wrapper = makeTable(1, { sortKey: 2, ascending: true });

      sortHeader = wrapper.find("th.sorted");
      assert.lengthOf(sortHeader, 1, "only a single column is sorted");
      assert.isTrue(sortHeader.is(".ascending"), "sorted column should be marked as ascending");
      assert.equal(sortHeader.text(), columns[1].title, "second column should be sorted.");
    });
  });

  describe("changes sort setting on clicks.", () => {
    it("sorts descending on initial click.", () => {
      let spy = sinon.spy();
      let wrapper = makeTable(1, undefined, spy);
      wrapper.find("th.column").first().simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue(spy.calledWith({
        sortKey: 1,
        ascending: false,
      } as SortSetting));
    });

    // Click on sorted data, different column.
    it("sorts descending on new column.", () => {
      let spy = sinon.spy();
      let wrapper = makeTable(1, {sortKey: 2, ascending: true}, spy);

      wrapper.find("th.column").first().simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue(spy.calledWith({
        sortKey: 1,
        ascending: false,
      } as SortSetting));
    });

    it("sorts ascending if same column is clicked twice.", () => {
      let spy = sinon.spy();
      let wrapper = makeTable(1, {sortKey: 1, ascending: false}, spy);

      wrapper.find("th.column").first().simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue( spy.calledWith({
        sortKey: 1,
        ascending: true,
      } as SortSetting));
    });

    it("removes sorting if same column is clicked thrice.", () => {
      let spy = sinon.spy();
      let wrapper = makeTable(1, {sortKey: 1, ascending: true}, spy);

      wrapper.find("th.column").first().simulate("click");
      assert.isTrue(spy.calledOnce);
      assert.isTrue( spy.calledWith({
        sortKey: null,
        ascending: false,
      } as SortSetting));
    });

    // Click on unsortable column does nothing.
    it("does nothing if unsortable column is clicked.", () => {
      let spy = sinon.spy();
      let wrapper = makeTable(1, {sortKey: 1, ascending: true}, spy);

      wrapper.find("th.column").last().simulate("click");
      assert.isTrue(spy.notCalled);
    });
  });
});
