// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { default as AntTable, ColumnProps } from "antd/es/table";
import ConfigProvider from "antd/es/config-provider";
import cn from "classnames";

import "antd/es/table/style/css";
import "./table.styl";

export type ColumnsConfig<T> = Array<ColumnProps<T>>;

export interface TableProps<T> {
  columns: Array<ColumnProps<T>>;
  dataSource: Array<T>;
  noDataMessage?: React.ReactNode;
  tableLayout?: "fixed" | "auto";
  pageSize?: number;
  className?: string;
}

const customizeRenderEmpty = (node: React.ReactNode) => () => (
  <div className="empty-table__message">{node}</div>
);

Table.defaultProps = {
  noDataMessage: "No data to display",
  tableLayout: "auto",
  className: "",
};

export function Table<T>(props: TableProps<T>) {
  const {
    columns,
    dataSource,
    noDataMessage,
    tableLayout,
    pageSize,
    className,
  } = props;
  return (
    <ConfigProvider renderEmpty={customizeRenderEmpty(noDataMessage)}>
      <AntTable<T>
        className={cn(`crl-table-wrapper ${className}`, {
          "crl-table-wrapper__empty": dataSource.length === 0,
        })}
        columns={columns}
        dataSource={dataSource}
        expandRowByClick
        tableLayout={tableLayout}
        pagination={{ hideOnSinglePage: true, pageSize }}
      />
    </ConfigProvider>
  );
}
