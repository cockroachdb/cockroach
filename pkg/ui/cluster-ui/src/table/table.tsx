import React from "react";
import { Table as AntTable, ConfigProvider } from "antd";
import { ColumnProps } from "antd/lib/table";
import classnames from "classnames/bind";
import styles from "./table.module.scss";

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

export function Table<T>(props: TableProps<T>) {
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
        expandRowByClick
        tableLayout={tableLayout}
        pagination={{ hideOnSinglePage: true, pageSize }}
        onChange={(pagination, filters, sorter) => {
          if (onSortingChange && sorter.column) {
            onSortingChange(
              sorter.column?.title as string,
              sorter.order === "ascend",
            );
          }
        }}
      />
    </ConfigProvider>
  );
}
