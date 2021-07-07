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
import * as Long from "long";
import { Moment } from "moment";
import { createSelector } from "reselect";

import times from "lodash/times";
import { EmptyPanel, EmptyPanelProps } from "../empty";
import styles from "./sortedtable.module.scss";
import classNames from "classnames/bind";
import { TableSpinner } from "./tableSpinner";
import { TableHead } from "./tableHead";
import { TableRow } from "./tableRow";
import { Tooltip } from "@cockroachlabs/ui-components";

export interface ISortedTablePagination {
  current: number;
  pageSize: number;
}

/**
 * ColumnDescriptor is used to describe metadata about an individual column
 * in a SortedTable.
 */
export interface ColumnDescriptor<T> {
  // Title string that should appear in the header column.
  title: React.ReactNode;
  // Function which generates the contents of an individual cell in this table.
  cell: (obj: T) => React.ReactNode;
  // Function which returns a value that can be used to sort the collection of
  // objects. This will be used to sort the table according to the data in
  // this column.
  // TODO(vilterp): using an "Ordered" typeclass here would be nice;
  // not sure how to do that in TypeScript.
  sort?: (obj: T) => string | number | Long | Moment;
  // Function that generates a "rollup" value for this column from all objects
  // in a collection. This is used to display an appropriate "total" value for
  // each column.
  rollup?: (objs: T[]) => React.ReactNode;
  // className to be applied to the td elements in this column.
  className?: string;
  titleAlign?: "left" | "right" | "center";
  // uniq column identifier
  name: string;
  // show or hide column by default; It can be overridden by users settings. True if not defined.
  showByDefault?: boolean;
  // If true, the user can't overwrite the setting for this column. False if not defined.
  alwaysShow?: boolean;
}

/**
 * SortedTableProps describes the properties expected by a SortedTable
 * component.
 */
interface SortedTableProps<T> {
  // The data which should be displayed in the table. This data may be sorted
  // by this component before display.
  data: T[];
  // Description of columns to display.
  columns: ColumnDescriptor<T>[];
  // sortSetting specifies how data should be sorted in this table, by
  // specifying a column id and a direction.
  sortSetting: SortSetting;
  // Callback that should be invoked when the user want to change the sort
  // setting.
  onChangeSortSetting?: { (ss: SortSetting): void };
  // className to be applied to the table element.
  className?: string;
  // A function that returns the class to apply to a given row.
  rowClass?: (obj: T) => string;

  // expandableConfig, if provided, makes each row in the table "expandable",
  // i.e. each row has an expand/collapse arrow on its left, and renders
  // a full-width area below it when expanded.
  expandableConfig?: {
    // expandedContent returns the content for a row's full-width expanded
    // section, given the object that row represents.
    expandedContent: (obj: T) => React.ReactNode;
    // expansionKey returns a key used to uniquely identify a row for the
    // purposes of tracking whether it's expanded or not.
    expansionKey: (obj: T) => string;
  };
  firstCellBordered?: boolean;
  renderNoResult?: React.ReactNode;
  pagination?: ISortedTablePagination;
  loading?: boolean;
  loadingLabel?: string;
  // empty state for table
  empty?: boolean;
  emptyProps?: EmptyPanelProps;
}

interface SortedTableState {
  expandedRows: Set<string>;
}

/**
 * SortedTable displays data rows in a table which can be sorted by the values
 * in a single column. Unsorted row data is passed to SortedTable along with
 * a SortSetting; SortedTable uses a selector to sort the data for display.
 *
 * SortedTable also computes optional "rollup" values for each column; a rollup
 * is a total value that is computed for a column based on all available rows.
 *
 * SortedTable should be preferred over the lower-level SortableTable when
 * all data rows to be displayed are available locally on the client side.
 */

const cx = classNames.bind(styles);

/**
 * SortableColumn describes the contents a single column of a
 * sortable table.
 */
export interface SortableColumn {
  // Text that will appear in the title header of the table.
  title: React.ReactNode;
  // Function which provides the contents for this column for a given row index
  // in the dataset.
  cell: (rowIndex: number) => React.ReactNode;
  // Contents that will appear in the "rollup" header of this column.
  rollup?: React.ReactNode;
  // Unique key that identifies this column from others, for the purpose of
  // indicating sort order. If not provided, the column is not considered
  // sortable.
  columnTitle?: any;
  // className is a classname to apply to the td elements
  className?: string;
  titleAlign?: "left" | "right" | "center";
  // uniq column identifier
  name: string;
}

/**
 * SortSetting is the structure that SortableTable uses to indicate its current
 * sort preference to higher-level components. It contains a columnTitle (taken from
 * one of the currently displayed columns) and a boolean indicating that the
 * sort should be ascending, rather than descending.
 */
export interface SortSetting {
  ascending: boolean;
  columnTitle?: string;
}

export interface ExpandableConfig {
  // Called when the expand toggle is clicked. If this prop is not supplied,
  // the table is not expandable and the expansion control is not shown.
  onChangeExpansion: (rowIndex: number, expanded: boolean) => void;
  // Given a row index, return whether that row is expanded.
  rowIsExpanded: (rowIndex: number) => boolean;
  // If a row is expanded, this function is called to get the content for the
  // full-width expanded section.
  expandedContent: (rowIndex: number) => React.ReactNode;
}

/**
 * SortableTable is designed to display tabular data where the data set can be
 * sorted by one or more columns.
 *
 * SortableTable is not responsible for sorting data; however, it does allow the
 * user to indicate how data should be sorted by clicking on column headers.
 * SortableTable can indicate this to higher-level components through the
 * 'onChangeSortSetting' callback property.
 */

export class SortedTable<T> extends React.Component<
  SortedTableProps<T>,
  SortedTableState
> {
  static defaultProps: Partial<SortedTableProps<any>> = {
    rowClass: (_obj: any) => "",
    columns: [],
    sortSetting: {
      ascending: false,
      columnTitle: null,
    },
    onChangeSortSetting: _ss => {},
  };

  rollups = createSelector(
    (props: SortedTableProps<T>) => props.data,
    (props: SortedTableProps<T>) => props.columns,
    (data: T[], columns: ColumnDescriptor<T>[]) => {
      return _.map(
        columns,
        (c): React.ReactNode => {
          if (c.rollup) {
            return c.rollup(data);
          }
          return undefined;
        },
      );
    },
  );

  sortedAndPaginated = createSelector(
    (props: SortedTableProps<T>) => props.data,
    (props: SortedTableProps<T>) => props.sortSetting,
    (props: SortedTableProps<T>) => props.columns,
    (props: SortedTableProps<T>) => props.pagination,
    (
      data: T[],
      sortSetting: SortSetting,
      columns: ColumnDescriptor<T>[],
      pagination?: ISortedTablePagination,
    ): T[] => {
      if (!sortSetting) {
        return this.paginatedData();
      }
      const sortColumn = columns.filter(
        c => c.name === sortSetting.columnTitle,
      )[0];
      if (!sortColumn || !sortColumn.sort) {
        return this.paginatedData();
      }
      return this.paginatedData(
        _.orderBy(
          data,
          sortColumn.sort,
          sortSetting.ascending ? "asc" : "desc",
        ),
      );
    },
  );

  /**
   * columns is a selector which computes the input columns to the underlying
   * sortableTable.
   */

  columns = createSelector(
    this.sortedAndPaginated,
    this.rollups,
    (props: SortedTableProps<T>) => props.columns,
    (
      sorted: T[],
      rollups: React.ReactNode[],
      columns: ColumnDescriptor<T>[],
    ) => {
      return _.map(
        columns,
        (cd, ii): SortableColumn => {
          return {
            name: cd.name,
            title: cd.title,
            cell: index => cd.cell(sorted[index]),
            columnTitle: cd.sort ? cd.name : undefined,
            rollup: rollups[ii],
            className: cd.className,
            titleAlign: cd.titleAlign,
          };
        },
      );
    },
  );

  rowClass = createSelector(
    this.sortedAndPaginated,
    (props: SortedTableProps<T>) => props.rowClass,
    (sorted: T[], rowClass: (obj: T) => string) => {
      return (index: number) => rowClass(sorted[index]);
    },
  );

  // TODO(vilterp): use a LocalSetting instead so the expansion state
  // will persist if the user navigates to a different page and back.
  state: SortedTableState = {
    expandedRows: new Set<string>(),
  };

  getItemAt(rowIndex: number): T {
    const sorted = this.sortedAndPaginated(this.props);
    return sorted[rowIndex];
  }

  getKeyAt(rowIndex: number): string {
    return this.props.expandableConfig.expansionKey(this.getItemAt(rowIndex));
  }

  onChangeExpansion = (rowIndex: number, expanded: boolean) => {
    const key = this.getKeyAt(rowIndex);
    const expandedRows = this.state.expandedRows;
    if (expanded) {
      expandedRows.add(key);
    } else {
      expandedRows.delete(key);
    }
    this.setState({
      expandedRows: expandedRows,
    });
  };

  rowIsExpanded = (rowIndex: number): boolean => {
    const key = this.getKeyAt(rowIndex);
    return this.state.expandedRows.has(key);
  };

  expandedContent = (rowIndex: number): React.ReactNode => {
    const item = this.getItemAt(rowIndex);
    return this.props.expandableConfig.expandedContent(item);
  };

  paginatedData = (sortData?: T[]) => {
    const { pagination, data } = this.props;
    if (!pagination) {
      return sortData || data;
    }
    const currentDefault = pagination.current - 1;
    const start = currentDefault * pagination.pageSize;
    const end = currentDefault * pagination.pageSize + pagination.pageSize;
    return sortData ? sortData.slice(start, end) : data.slice(start, end);
  };

  render() {
    const {
      data,
      loading,
      sortSetting,
      onChangeSortSetting,
      firstCellBordered,
      renderNoResult,
      loadingLabel,
      empty,
      emptyProps,
      className,
    } = this.props;
    let expandableConfig: ExpandableConfig = null;
    if (this.props.expandableConfig) {
      expandableConfig = {
        expandedContent: this.expandedContent,
        rowIsExpanded: this.rowIsExpanded,
        onChangeExpansion: this.onChangeExpansion,
      };
    }

    const count = data ? this.paginatedData().length : 0;
    const columns = this.columns(this.props);
    const rowClass = this.rowClass(this.props);
    const tableWrapperClass = cx("cl-table-wrapper");
    const tableStyleClass = cx("sort-table", className);
    const noResultsClass = cx("table__no-results");

    if (empty) {
      return <EmptyPanel {...emptyProps} />;
    }

    return (
      <div className={tableWrapperClass}>
        <table className={tableStyleClass}>
          <TableHead
            columns={columns}
            sortSetting={sortSetting}
            onChangeSortSetting={onChangeSortSetting}
            expandableConfig={expandableConfig}
            firstCellBordered={firstCellBordered}
          />
          <tbody>
            {!loading &&
              times(count, (rowIndex: number) => (
                <TableRow
                  key={"row" + rowIndex}
                  columns={columns}
                  expandableConfig={expandableConfig}
                  firstCellBordered={firstCellBordered}
                  rowIndex={rowIndex}
                  rowClass={rowClass}
                />
              ))}
          </tbody>
        </table>
        {loading && <TableSpinner loadingLabel={loadingLabel} />}
        {count === 0 && <div className={noResultsClass}>{renderNoResult}</div>}
      </div>
    );
  }
}

/**
 * Creates an element limited by the max length and
 * with a tooltip with one listed element per line.
 * E.g. `value1, value2, value3` with maxLength 10 will display
 * `value1, va...` on the table and
 * `value1
 * value2
 * value3`  on the tooltip.
 * @param value a string with elements separated by `, `.
 * @param maxLength the max length to which it should display value
 * and hide the remaining.
 */
export function longListWithTooltip(value: string, maxLength: number) {
  const summary =
    value.length > maxLength ? value.slice(0, maxLength) + "..." : value;
  return (
    <Tooltip
      placement="bottom"
      content={
        <pre className={cx("break-line")}>{value.split(", ").join("\r\n")}</pre>
      }
    >
      <div className="cl-table-link__tooltip-hover-area">{summary}</div>
    </Tooltip>
  );
}
