import React from "react";
import { default as AntTable, ColumnProps } from "antd/lib/table";
import ConfigProvider from "antd/lib/config-provider";
import classnames from "classnames/bind";

import "antd/lib/table/style/css";
import styles from "./table.module.scss";

export type ColumnsConfig<T> = Array<ColumnProps<T>>;

export interface TableProps<T> {
  columns: Array<ColumnProps<T>>;
  dataSource: Array<T>;
  noDataMessage?: React.ReactNode;
  tableLayout?: "fixed" | "auto";
  pageSize?: number;
  className?: string;
}

const cx = classnames.bind(styles);

const customizeRenderEmpty = (node: React.ReactNode) => () => (
  <div className={cx("empty-table__message")}>{node}</div>
);

export function Table<T>(props: TableProps<T>) {
  const {
    columns,
    dataSource,
    noDataMessage = "No data to display",
    tableLayout = "auto",
    pageSize,
    className,
  } = props;
  return (
    <ConfigProvider renderEmpty={customizeRenderEmpty(noDataMessage)}>
      <AntTable<T>
        className={cx(`crl-table-wrapper ${className}`, {
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
