// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as  React from "react";
import { default as AntTable, ColumnProps } from "antd/es/table";

import "antd/es/table/style/css";
import "./table.styl";

export interface ColumnsConfig<T> extends Array<ColumnProps<T>> {}

export interface TableProps<T> {
  columns: Array<ColumnProps<T>>;
  dataSource: Array<T>;
}

export function Table<T>(props: TableProps<T>) {
  const { columns, dataSource } = props;

  return (
    <AntTable<T>
      className="crl-table-wrapper"
      columns={columns}
      dataSource={dataSource}
      pagination={{hideOnSinglePage: true}}
      indentSize={32} />
  );
}
