// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import { History } from "history";
import orderBy from "lodash/orderBy";
import times from "lodash/times";
import * as Long from "long";
import { Moment } from "moment-timezone";
import React, { useState, useMemo, useCallback } from "react";

import { EmptyPanel, EmptyPanelProps } from "../empty";

import styles from "./sortedtable.module.scss";
import { TableHead } from "./tableHead";
import { TableRow } from "./tableRow";
import { TableSpinner } from "./tableSpinner";

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
  // Hides the dashed title underline, which represents that a tooltip exists.
  // Defaults to false and shows the underline if not defined.
  hideTitleUnderline?: boolean;
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
  // If true, hide this column for tenant clusters. False if not defined.
  hideIfTenant?: boolean;
}

/**
 * SortedTableProps describes the properties expected by a SortedTable
 * component.
 */
export interface SortedTableProps<T> {
  // The data which should be displayed in the table. This data may be sorted
  // by this component before display. Defaults to an empty array if not provided.
  data?: T[];
  // Description of columns to display.
  columns: ColumnDescriptor<T>[];
  // sortSetting specifies how data should be sorted in this table, by
  // specifying a column id and a direction.
  sortSetting?: SortSetting;
  // Callback that should be invoked when the user want to change the sort
  // setting.
  onChangeSortSetting?: { (ss: SortSetting): void };
  // className to be applied to the table element.
  className?: string;
  // tableWrapperClassName is a class name applied to table wrapper.
  tableWrapperClassName?: string;
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
  disableSortSizeLimit?: number;
  // empty state for table
  empty?: boolean;
  emptyProps?: EmptyPanelProps;
  dataTestId?: string;
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
  // Hides the dashed title underline, which represents that a tooltip exists.
  // Defaults to false and shows the underline if not defined.
  hideTitleUnderline?: boolean;
  // Function which provides the contents for this column for a given row index
  // in the dataset.
  cell: (rowIndex: number) => React.ReactNode;
  // Contents that will appear in the "rollup" header of this column.
  rollup?: React.ReactNode;
  // Unique key that identifies this column from others, for the purpose of
  // indicating sort order. If not provided, the column is not considered
  // sortable.
  columnTitle?: string;
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

const defaultRowClass = (_obj: unknown) => "";
const defaultOnChangeSortSetting = (_ss: SortSetting) => {};
const defaultSortSetting: SortSetting = {
  ascending: false,
  columnTitle: null,
};

export function SortedTable<T>({
  data = [] as T[],
  columns: columnDescriptors = [],
  sortSetting = defaultSortSetting,
  onChangeSortSetting = defaultOnChangeSortSetting,
  className,
  tableWrapperClassName,
  rowClass = defaultRowClass,
  expandableConfig: expandableConfigProp,
  firstCellBordered,
  renderNoResult,
  pagination,
  loading,
  loadingLabel,
  disableSortSizeLimit,
  empty,
  emptyProps,
  dataTestId,
}: SortedTableProps<T>): React.ReactElement {
  // TODO(vilterp): use a LocalSetting instead so the expansion state
  // will persist if the user navigates to a different page and back.
  const [expandedRows, setExpandedRows] = useState<Set<string>>(
    () => new Set<string>(),
  );

  const paginatedData = useCallback(
    (sortData?: T[]): T[] => {
      if (!pagination) {
        return sortData || data;
      }
      const currentDefault = pagination.current - 1;
      const start = currentDefault * pagination.pageSize;
      const end = currentDefault * pagination.pageSize + pagination.pageSize;
      return sortData ? sortData.slice(start, end) : data.slice(start, end);
    },
    [data, pagination],
  );

  const rollups = useMemo((): React.ReactNode[] => {
    return columnDescriptors.map((c): React.ReactNode => {
      if (c.rollup) {
        return c.rollup(data);
      }
      return undefined;
    });
  }, [data, columnDescriptors]);

  const sortedAndPaginatedData = useMemo((): T[] => {
    if (!sortSetting) {
      return paginatedData();
    }

    if (disableSortSizeLimit && data.length > disableSortSizeLimit) {
      return paginatedData();
    }

    const sortColumn = columnDescriptors.find(
      c => c.name === sortSetting.columnTitle,
    );
    if (!sortColumn || !sortColumn.sort) {
      return paginatedData();
    }
    return paginatedData(
      orderBy(data, sortColumn.sort, sortSetting.ascending ? "asc" : "desc"),
    );
  }, [
    data,
    sortSetting,
    columnDescriptors,
    disableSortSizeLimit,
    paginatedData,
  ]);

  const columns = useMemo((): SortableColumn[] => {
    const sort = !disableSortSizeLimit || data.length <= disableSortSizeLimit;

    return columnDescriptors.map((cd, ii): SortableColumn => {
      return {
        name: cd.name,
        title: cd.title,
        hideTitleUnderline: cd.hideTitleUnderline,
        cell: index => cd.cell(sortedAndPaginatedData[index]),
        columnTitle: sort && cd.sort ? cd.name : undefined,
        rollup: rollups[ii],
        className: cd.className,
        titleAlign: cd.titleAlign,
      };
    });
  }, [
    sortedAndPaginatedData,
    rollups,
    columnDescriptors,
    disableSortSizeLimit,
    data.length,
  ]);

  const rowClassFn = useMemo((): ((index: number) => string) => {
    return (index: number) => rowClass(sortedAndPaginatedData[index]);
  }, [sortedAndPaginatedData, rowClass]);

  let expandableConfig: ExpandableConfig = null;
  if (expandableConfigProp) {
    const getKeyAt = (rowIndex: number): string => {
      return expandableConfigProp.expansionKey(
        sortedAndPaginatedData[rowIndex],
      );
    };

    expandableConfig = {
      expandedContent: (rowIndex: number): React.ReactNode => {
        return expandableConfigProp.expandedContent(
          sortedAndPaginatedData[rowIndex],
        );
      },
      rowIsExpanded: (rowIndex: number): boolean => {
        return expandedRows.has(getKeyAt(rowIndex));
      },
      onChangeExpansion: (rowIndex: number, expanded: boolean): void => {
        const key = getKeyAt(rowIndex);
        setExpandedRows(prev => {
          const next = new Set(prev);
          if (expanded) {
            next.add(key);
          } else {
            next.delete(key);
          }
          return next;
        });
      },
    };
  }

  const count = data ? paginatedData().length : 0;
  const tableWrapperClass = cx("cl-table-wrapper", tableWrapperClassName);
  const tableStyleClass = cx("sort-table", className);
  const noResultsClass = cx("table__no-results");

  if (empty) {
    return <EmptyPanel {...emptyProps} />;
  }

  return (
    <div className={tableWrapperClass}>
      <table className={tableStyleClass} data-testid={dataTestId}>
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
                rowClass={rowClassFn}
              />
            ))}
        </tbody>
      </table>
      {loading && <TableSpinner loadingLabel={loadingLabel} />}
      {!loading && count === 0 && (
        <div className={noResultsClass}>{renderNoResult}</div>
      )}
    </div>
  );
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
export function longListWithTooltip(
  value: string,
  maxLength: number,
): React.ReactElement {
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

/**
 * Get Sort Setting from Query String and if it's different from current
 * sortSetting calls the onSortChange function.
 * @param page the page where the table was added (used for analytics)
 * @param queryString searchParams
 * @param sortSetting the current sort Setting on the page
 * @param onSortingChange function to be called if the values from the search
 * params are different from the current ones. This function can update
 * the value stored on localStorage for example.
 */
export const handleSortSettingFromQueryString = (
  page: string,
  queryString: string,
  sortSetting: SortSetting,
  onSortingChange: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void,
): void => {
  const searchParams = new URLSearchParams(queryString);
  const ascending = (searchParams.get("ascending") || undefined) === "true";
  const columnTitle = searchParams.get("columnTitle") || undefined;
  if (
    onSortingChange &&
    columnTitle &&
    (sortSetting.columnTitle !== columnTitle ||
      sortSetting.ascending !== ascending)
  ) {
    onSortingChange(page, columnTitle, ascending);
  }
};

/**
 * Update the query params to the current values of the Sort Setting.
 * When we change tabs inside the SQL Activity page for example,
 * the constructor is called only on the first time.
 * The component update event is called frequently and can be used to
 * update the query params by using this function that only updates
 * the query params if the values did change and we're on the correct tab.
 * @param tab which the query params should update
 * @param sortSetting the current sort settings
 * @param defaultSortSetting the default sort settings
 * @param history
 */
export const updateSortSettingQueryParamsOnTab = (
  tab: string,
  sortSetting: SortSetting,
  defaultSortSetting: SortSetting,
  history: History,
): void => {
  const searchParams = new URLSearchParams(history.location.search);
  const currentTab = searchParams.get("tab") || "";
  const ascending =
    (searchParams.get("ascending") ||
      defaultSortSetting.ascending.toString()) === "true";
  const columnTitle =
    searchParams.get("columnTitle") || defaultSortSetting.columnTitle;
  if (
    currentTab === tab &&
    (sortSetting.columnTitle !== columnTitle ||
      sortSetting.ascending !== ascending)
  ) {
    const params = {
      ascending: sortSetting.ascending.toString(),
      columnTitle: sortSetting.columnTitle,
    };
    const nextSearchParams = new URLSearchParams(history.location.search);
    Object.entries(params).forEach(([key, value]) => {
      if (!value) {
        nextSearchParams.delete(key);
      } else {
        nextSearchParams.set(key, value);
      }
    });
    history.location.search = nextSearchParams.toString();
    history.replace(history.location);
  }
};
