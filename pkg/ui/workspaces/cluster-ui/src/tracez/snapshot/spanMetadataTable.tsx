// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Nodes } from "@cockroachlabs/icons";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useState } from "react";

import { NamedOperationMetadata } from "src/api/tracezApi";
import { EmptyTable } from "src/empty";
import { CircleFilled } from "src/icon";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";

import styles from "../snapshot.module.scss";

import { formatDurationHours } from "./spanTable";

const cx = classNames.bind(styles);

class SpanMetadataSortedTable extends SortedTable<NamedOperationMetadata> {}

const columns: ColumnDescriptor<NamedOperationMetadata>[] = [
  {
    name: "icons",
    title: "",
    cell: row => {
      return row.metadata.contains_unfinished ? (
        <Tooltip title="At least one span unfinished" placement="bottom">
          <CircleFilled
            className={cx("icon-hollow-green")}
            viewBox={"-1 -1 12 12"}
          />
        </Tooltip>
      ) : null;
    },
    className: cx("metadata-icon-cell"),
  },
  {
    name: "name",
    title: "Name",
    cell: row => row.name,
    sort: row => row.name,
    hideTitleUnderline: true,
    className: cx("metadata-name-cell"),
  },
  {
    name: "count",
    title: "Count",
    cell: row => row.metadata.count.toNumber(),
    sort: row => row.metadata.count,
    hideTitleUnderline: true,
    className: cx("table-cell"),
  },
  {
    name: "duration",
    title: "Duration",
    cell: row =>
      formatDurationHours(
        moment.duration(row.metadata.duration.toNumber() * 1e-6),
      ),
    sort: row => row.metadata.duration,
    hideTitleUnderline: true,
    className: cx("table-cell"),
  },
];

export interface SpanMetadataTableProps {
  childrenMetadata: NamedOperationMetadata[];
}

export const SpanMetadataTable: React.FC<SpanMetadataTableProps> = props => {
  const { childrenMetadata } = props;
  const [sortSetting, setSortSetting] = useState<SortSetting>();

  if (!childrenMetadata) {
    return <EmptyTable title="No spans to show" icon={<Nodes />} />;
  }

  return (
    <SpanMetadataSortedTable
      data={childrenMetadata}
      sortSetting={sortSetting}
      onChangeSortSetting={setSortSetting}
      columns={columns}
      rowClass={() => cx("table-row")}
    />
  );
};
