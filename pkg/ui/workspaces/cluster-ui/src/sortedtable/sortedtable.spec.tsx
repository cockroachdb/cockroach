// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, fireEvent } from "@testing-library/react";
import sortBy from "lodash/sortBy";
import sumBy from "lodash/sumBy";
import React from "react";

import {
  SortedTable,
  ColumnDescriptor,
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable";

class TestRow {
  constructor(
    public name: string,
    public value: number,
  ) {}
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
    rollup: trs => sumBy(trs, tr => tr.value),
  },
];

function renderTable(
  data: TestRow[],
  sortSetting?: SortSetting,
  onChangeSortSetting?: (ss: SortSetting) => void,
  pagination?: ISortedTablePagination,
) {
  return render(
    <SortedTable<TestRow>
      data={data}
      sortSetting={sortSetting}
      onChangeSortSetting={onChangeSortSetting}
      pagination={pagination}
      columns={columns}
    />,
  );
}

function renderExpandableTable(data: TestRow[], sortSetting: SortSetting) {
  return render(
    <SortedTable<TestRow>
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

/**
 * Get the text content of each cell in each row of a tbody.
 */
function getBodyRowContents(container: HTMLElement): Array<Array<string>> {
  const tbody = container.querySelector("tbody");
  if (!tbody) return [];
  const rows = tbody.querySelectorAll("tr");
  return Array.from(rows).map(tr => {
    const cells = tr.querySelectorAll("td");
    return Array.from(cells).map(td => td.textContent || "");
  });
}

describe("<SortedTable>", () => {
  it("renders the expected table structure.", () => {
    const { container } = renderTable([new TestRow("test", 1)]);

    expect(container.querySelectorAll("table")).toHaveLength(1);
    expect(container.querySelectorAll("thead tr")).toHaveLength(1);
    expect(
      container.querySelectorAll("tr.head-wrapper__row--header"),
    ).toHaveLength(1);
    expect(container.querySelectorAll("tbody")).toHaveLength(1);
  });

  it("correctly uses onChangeSortSetting", () => {
    const spy = jest.fn();
    const { container } = renderTable([new TestRow("test", 1)], undefined, spy);

    const headerCells = container.querySelectorAll("th.head-wrapper__cell");
    fireEvent.click(headerCells[0]);

    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledWith({
      ascending: false,
      columnTitle: "first",
    });
  });

  it("correctly sorts data based on sortSetting", () => {
    const data = [
      new TestRow("c", 3),
      new TestRow("d", 4),
      new TestRow("a", 1),
      new TestRow("b", 2),
    ];

    const assertMatches = (container: HTMLElement, expected: TestRow[]) => {
      const tbody = container.querySelector("tbody");
      expected.forEach((rowData, dataIndex) => {
        const row = tbody.children[dataIndex];
        const cells = row.querySelectorAll("td");
        expect(cells[0].textContent).toBe(rowData.name);
        expect(cells[1].textContent).toBe(rowData.value.toString());
      });
    };

    // Unsorted
    let { container } = renderTable(data, undefined);
    assertMatches(container, data);

    // Sorted by first column ascending
    ({ container } = renderTable(data, {
      ascending: true,
      columnTitle: "first",
    }));
    assertMatches(
      container,
      sortBy(data, r => r.name),
    );

    // Rerender with sort by second column
    ({ container } = renderTable(data, {
      ascending: true,
      columnTitle: "second",
    }));
    assertMatches(
      container,
      sortBy(data, r => r.value),
    );
  });

  describe("with expandableConfig", () => {
    it("renders the expected table structure", () => {
      const { container } = renderExpandableTable(
        [new TestRow("test", 1)],
        undefined,
      );

      expect(container.querySelectorAll("table")).toHaveLength(1);
      expect(container.querySelectorAll("thead tr")).toHaveLength(1);
      expect(
        container.querySelectorAll("tr.head-wrapper__row--header"),
      ).toHaveLength(1);
      expect(container.querySelectorAll("tbody")).toHaveLength(1);
      expect(container.querySelectorAll("tbody tr")).toHaveLength(1);
      // Two body cells plus one expansion control cell
      expect(container.querySelectorAll("tbody td")).toHaveLength(3);
      expect(
        container.querySelectorAll("td.row-wrapper__cell__expansion-control"),
      ).toHaveLength(1);
    });

    it("expands and collapses the clicked row", () => {
      const { container } = renderExpandableTable(
        [new TestRow("test", 1)],
        undefined,
      );

      // Nothing expanded yet
      expect(
        container.querySelectorAll(".row-wrapper__row--expanded-area"),
      ).toHaveLength(0);

      // Click to expand
      const expansionControl = container.querySelector(
        ".row-wrapper__cell__expansion-control",
      );
      fireEvent.click(expansionControl);

      // Row is expanded
      const expandedArea = container.querySelector(
        ".row-wrapper__row--expanded-area",
      );
      expect(expandedArea).toBeInTheDocument();

      // Expanded row contains placeholder and content area
      const expandedChildren = expandedArea.querySelectorAll("td");
      expect(expandedChildren).toHaveLength(2);

      // First td is empty placeholder
      expect(expandedChildren[0].innerHTML).toBe("");

      // Second td contains the expanded content
      expect(expandedChildren[1].textContent).toBe("test=1");
      expect(expandedChildren[1].getAttribute("colspan")).toBe("2");

      // Click to collapse
      fireEvent.click(expansionControl);
      expect(
        container.querySelectorAll(".row-wrapper__row--expanded-area"),
      ).toHaveLength(0);
    });
  });

  it("should correctly render rows with pagination and sort settings", () => {
    const data = [
      new TestRow("c", 3),
      new TestRow("d", 4),
      new TestRow("a", 1),
      new TestRow("b", 2),
    ];

    // Page 1, unsorted
    let { container } = renderTable(data, undefined, undefined, {
      current: 1,
      pageSize: 2,
    });
    let tbody = container.querySelector("tbody");
    expect(container.querySelectorAll("tbody tr")).toHaveLength(2);
    expect(tbody.children[1].querySelector("td").textContent).toBe("d");

    // Page 2, unsorted
    ({ container } = renderTable(data, undefined, undefined, {
      current: 2,
      pageSize: 2,
    }));
    tbody = container.querySelector("tbody");
    expect(container.querySelectorAll("tbody tr")).toHaveLength(2);
    expect(tbody.children[0].querySelector("td").textContent).toBe("a");

    // Page 1, sorted by first column
    ({ container } = renderTable(
      data,
      { ascending: true, columnTitle: "first" },
      undefined,
      {
        current: 1,
        pageSize: 2,
      },
    ));
    tbody = container.querySelector("tbody");
    expect(tbody.children[1].querySelector("td").textContent).toBe("b");

    // Page 2, sorted by first column
    ({ container } = renderTable(
      data,
      { ascending: true, columnTitle: "first" },
      undefined,
      {
        current: 2,
        pageSize: 2,
      },
    ));
    tbody = container.querySelector("tbody");
    expect(tbody.children[0].querySelector("td").textContent).toBe("c");
  });

  it("should update when pagination changes", () => {
    const data = [
      new TestRow("c", 3),
      new TestRow("d", 4),
      new TestRow("a", 1),
      new TestRow("b", 2),
    ];

    const { container, rerender } = renderTable(data, undefined, undefined, {
      current: 1,
      pageSize: 2,
    });

    expect(getBodyRowContents(container)).toEqual([
      ["c", "3"],
      ["d", "4"],
    ]);

    rerender(
      <SortedTable<TestRow>
        data={data}
        sortSetting={undefined}
        onChangeSortSetting={undefined}
        pagination={{ current: 2, pageSize: 2 }}
        columns={columns}
      />,
    );

    expect(getBodyRowContents(container)).toEqual([
      ["a", "1"],
      ["b", "2"],
    ]);
  });
});
