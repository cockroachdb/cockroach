// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretDownOutlined, CaretRightOutlined } from "@ant-design/icons";
import { Table as AntTable, ConfigProvider } from "antd";
import classnames from "classnames/bind";
import isArray from "lodash/isArray";
import React from "react";

import styles from "./table.module.scss";

import type { ColumnProps } from "antd/es/table";

export type ColumnsConfig<T> = Array<ColumnProps<T>>;

export interface TableProps<T> {
  columns: Array<ColumnProps<T>>;
  dataSource: Array<T>;
  noDataMessage?: React.ReactNode;
  tableLayout?: "fixed" | "auto";
  pageSize?: number;
  className?: string;
  onSortingChange?: (columnName: string, ascending: boolean) => void;
}

const cx = classnames.bind(styles);

const customizeRenderEmpty = (node: React.ReactNode) => () => (
  <div className={cx("empty-table__message")}>{node}</div>
);

export function Table<T extends object & { children?: T[] }>(
  props: TableProps<T>,
): React.ReactElement {
  const {
    columns,
    dataSource,
    noDataMessage = "No data to display",
    tableLayout = "auto",
    pageSize,
    className,
    onSortingChange,
  } = props;
  return (
    <ConfigProvider renderEmpty={customizeRenderEmpty(noDataMessage)}>
      <AntTable<T>
        className={cx("crl-table-wrapper", className, {
          "crl-table-wrapper__empty": dataSource.length === 0,
        })}
        columns={columns}
        dataSource={dataSource}
        tableLayout={tableLayout}
        pagination={{ hideOnSinglePage: true, pageSize }}
        onChange={(_pagination, _filters, sorter) => {
          if (onSortingChange && !isArray(sorter)) {
            onSortingChange(
              sorter.column?.title as string,
              sorter.order === "ascend",
            );
          }
        }}
        expandable={{
          expandRowByClick: true,
          expandIcon: ({ expanded, onExpand, record }) => {
            // Don't render expand icon for row that doesn't have children elements.
            if (!(record.children && record.children.length > 0)) {
              return null;
            }
            return expanded ? (
              <CaretDownOutlined
                onClick={e => onExpand(record, e)}
                className={cx("expand-toggle")}
              />
            ) : (
              <CaretRightOutlined
                onClick={e => onExpand(record, e)}
                className={cx("expand-toggle")}
              />
            );
          },
        }}
      />
    </ConfigProvider>
  );
}
