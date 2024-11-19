// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Table as AntDTable,
  TooltipProps,
  TableProps as AntDTableProps,
} from "antd";
import { ColumnType, ColumnsType, TablePaginationConfig } from "antd/lib/table";
import { CompareFn, SorterResult } from "antd/lib/table/interface";
import classNames from "classnames";
import React, { useMemo } from "react";

import emptyListResultsImg from "src/assets/emptyState/empty-list-results.svg";

import { Alert, AlertType } from "./alert";
import DelayedLoaderIcon from "./delayedLoaderIcon";

import "./table.scss";

/**
 * useKeyedData is hook that memoizes listed data and selects an attribute
 * to set as a `key` for each item in the list. This allows us to easily render
 * a list within a table where each item has a unique key.
 *
 * @param data The list of data to be keyed.
 * @param key The attribute to use as the key, this needs to be a string.
 * @returns an object that extends the original data with a `key` attribute.
 */
export const useKeyedData = <T extends object>(
  data: T[] | undefined,
  key: keyof T,
) => {
  return useMemo(() => {
    if (!data) return undefined;
    return data.map(item => ({
      ...item,
      key: item[key] as string,
    }));
  }, [data, key]);
};

export interface TableColumnProps<T> {
  title: React.ReactNode;
  width?: number | string;
  sortDirections?: SortDirection[];
  defaultSortOrder?: SortDirection;
  sorter?: boolean | CompareFn<T>;
  sortOrder?: SortDirection;
  className?: string;
  align?: "left" | "center" | "right";
  showSorterTooltip?: boolean | TooltipProps;
  render: (record: T, idx: number) => React.ReactNode;
}

export declare type TableChangeFn<T> = (
  pagination: TablePaginationConfig,
  sorter: SorterResult<T>,
) => void;

export interface TableProps<T> {
  columns: TableColumnProps<T>[];
  dataSource: T[];
  title?: string;
  dataCount?: number;
  actionButton?: React.ReactNode;
  emptyState?: React.ReactElement;
  defaultExpandAllRows?: boolean;
  pagination?: TablePaginationConfig | false;
  rowClassName?: string | ((record: T, idx: number) => string);
  loadingClassName?: string;
  innerContainerClassName?: string;
  actionButtonClassName?: string;
  tableLayoutClassName?: string;
  onChange?: TableChangeFn<T>;
  loading?: boolean;
  error?: Error;
  errorState?: React.ReactElement;
  size?: AntDTableProps<T>["size"];
  darkmode?: boolean;
}

export interface StateTemplateProps {
  icon?: React.ReactNode;
  title?: string;
  message?: string | React.ReactNode;
  action?: React.ReactNode;
  helpText?: React.ReactNode;
}

export type SortDirection = "ascend" | "descend";

type KeyedTableData<T> = {
  key: string;
  children?: KeyedTableData<T>[];
} & T;

function mapTableColumnsToAntDesignColumns<T>(
  tableColumns: Array<TableColumnProps<T>>,
): ColumnsType<T> {
  const antDTableColumns: ColumnsType<T> = tableColumns.map((tc, idx) => {
    const mapCol: ColumnType<T> = {
      key: idx,
      title: tc.title,
      width: tc.width,
      align: tc.align,
      className: classNames(tc.className, {
        "crl-table__column--with-sorter-tooltip": tc.showSorterTooltip,
      }),
      showSorterTooltip: tc.showSorterTooltip,
      render: (_: unknown, record: T, index: number) =>
        tc.render(record, index),
    };

    if (tc.sorter) {
      mapCol.sorter = tc.sorter;
      mapCol.sortDirections = tc.sortDirections || ["ascend", "descend"];
      mapCol.defaultSortOrder = tc.defaultSortOrder;
      if (tc.sortOrder) {
        mapCol.sortOrder = tc.sortOrder;
      }
    }

    return mapCol;
  });

  return antDTableColumns;
}

const DEFAULT_EMPTY_ICON = <img src={emptyListResultsImg} />;

// export component for quick custom empty states
export const EmptyState = ({
  action,
  helpText,
  icon = DEFAULT_EMPTY_ICON,
  title = "No data loaded",
  message = "Data for this table does not exist yet.",
}: StateTemplateProps) => (
  <div className="crl-table__state-template">
    <div
      data-testid="empty-icon"
      className="crl-table__state-template-icon-container"
    >
      {icon}
    </div>
    <span className="crl-table__state-template-heading crl-subheading">
      {title}
    </span>
    <span className="crl-table__state-template-message">{message}</span>
    {action && <div className="crl-table__state-template-action">{action}</div>}
    {helpText && (
      <span
        className={action ? "crl-table__state-template-help-text" : undefined}
      >
        {helpText}
      </span>
    )}
  </div>
);

const DEFAULT_ERROR_ICON = (
  <img src="/assets/images/graphics/pictogram-alert.svg" />
);

// export component for quick custom error states
export const ErrorState = ({
  action,
  helpText,
  icon = DEFAULT_ERROR_ICON,
  title = "Data for this for this table is unavailable",
  message,
}: StateTemplateProps) => (
  <div className="crl-table__state-template">
    <div className="crl-table__state-template-icon-container">{icon}</div>
    <span className="crl-table__state-template-heading crl-subheading">
      {title}
    </span>
    {message && (
      <span className="crl-table__state-template-message">{message}</span>
    )}
    {action && <div className="crl-table__state-template-action">{action}</div>}
    {helpText && (
      <span className="crl-table__state-template-help-text">{helpText}</span>
    )}
  </div>
);

interface TableLayoutProps {
  title?: string;
  actionButton?: React.ReactNode;
  hasData: boolean;
  children: React.ReactNode;
  className?: string;
  darkmode?: boolean;
}

const TableLayout = ({
  title,
  actionButton,
  hasData,
  children,
  className,
  darkmode,
}: TableLayoutProps) => (
  <div
    className={classNames(
      "crl-table",
      {
        "crl-table--has-header": (title || actionButton) && hasData,
        "crl-table--darkmode": darkmode,
      },
      className,
    )}
  >
    {children}
  </div>
);

export function Table<T>({
  actionButton,
  columns,
  dataCount,
  dataSource,
  emptyState,
  pagination = false,
  rowClassName,
  loadingClassName,
  innerContainerClassName,
  actionButtonClassName,
  tableLayoutClassName,
  title,
  onChange,
  loading,
  error,
  errorState,
  size,
  darkmode,
}: TableProps<KeyedTableData<T>>) {
  if (error && errorState) {
    return (
      <TableLayout
        title={title}
        actionButton={actionButton}
        hasData={dataSource.length > 0}
      >
        {errorState}
      </TableLayout>
    );
  }

  if (error) {
    return (
      <Alert
        type={AlertType.DANGER}
        title="Failed to load"
        message={error.message}
      />
    );
  }

  if (loading) {
    return (
      <div className={`crl-table__loading ${loadingClassName}`}>
        <DelayedLoaderIcon />
      </div>
    );
  }

  if (dataSource.length === 0) {
    return (
      <TableLayout
        title={title}
        actionButton={actionButton}
        hasData={dataSource.length > 0}
      >
        {emptyState ? emptyState : <EmptyState />}
      </TableLayout>
    );
  }

  return (
    <TableLayout
      title={title}
      actionButton={actionButton}
      hasData={dataSource.length > 0}
      className={tableLayoutClassName}
      darkmode={darkmode}
    >
      <div className="crl-table__header">
        {(title || actionButton) && !loading && (
          <>
            <span className="crl-table__header-title">
              {title} {!!dataCount && <>({dataCount})</>}
            </span>
            {actionButton && (
              <div
                className={classNames(
                  "crl-table__header-action-buttons",
                  actionButtonClassName,
                )}
              >
                {actionButton}
              </div>
            )}
          </>
        )}
      </div>
      <div
        className={classNames(
          "crl-table__inner-container",
          innerContainerClassName,
        )}
      >
        <AntDTable
          columns={mapTableColumnsToAntDesignColumns(columns)}
          dataSource={dataSource}
          rowClassName={rowClassName}
          pagination={pagination}
          onChange={(pagination, filters, sorter) => {
            if (onChange) {
              const sorterResult = Array.isArray(sorter) ? sorter[0] : sorter;
              onChange(pagination, sorterResult);
            }
          }}
          showSorterTooltip={false}
          size={size}
        />
      </div>
    </TableLayout>
  );
}
