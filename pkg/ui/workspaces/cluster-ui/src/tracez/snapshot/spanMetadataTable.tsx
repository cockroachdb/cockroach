// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import moment from "moment";
import React, { useState } from "react";
import { Nodes, Caution, Plus, Minus } from "@cockroachlabs/icons";
import { Span, Snapshot } from "src/api/tracezApi";
import { EmptyTable } from "src/empty";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";

import styles from "../snapshot.module.scss";
import classNames from "classnames/bind";
import { TimestampToMoment } from "src/util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import ISpanTag = cockroach.server.serverpb.ISpanTag;
import RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;
import { CircleFilled } from "src/icon";
import { Dropdown } from "src/dropdown";
import "antd/lib/switch/style";
import { Link } from "react-router-dom";
import Long from "long";
import { formatDurationHours } from "./spanTable";
const cx = classNames.bind(styles);

export type SpanMetadataRow = {
  name: string;
  count: number;
  duration: number;
  containsUnfinished: boolean;
};

class SpanMetadataSortedTable extends SortedTable<SpanMetadataRow> {}

const columns: ColumnDescriptor<SpanMetadataRow>[] = [
  {
    name: "icons",
    title: "",
    cell: row => {
      return row.containsUnfinished ? (
        <CircleFilled className={cx("icon-green")} viewBox={"-1 -1 12 12"} />
      ) : null;
    },
    className: cx("metadata-icon-cell"),
  },
  {
    name: "name",
    title: "Name",
    cell: row => row.name,
    sort: row => row.count,
    hideTitleUnderline: true,
    className: cx("metadata-name-cell"),
  },
  {
    name: "count",
    title: "Count",
    cell: row => row.count,
    sort: row => row.count,
    hideTitleUnderline: true,
    className: cx("table-cell"),
  },
  {
    name: "duration",
    title: "Duration",
    cell: row => formatDurationHours(moment.duration(row.duration * 1e-6)),
    sort: row => row.duration,
    hideTitleUnderline: true,
    className: cx("table-cell"),
  },
];

export interface SpanMetadataTableProps {
  spanMetadataRows: SpanMetadataRow[];
}

export const SpanMetadataTable: React.FC<SpanMetadataTableProps> = props => {
  const { spanMetadataRows } = props;
  const [sortSetting, setSortSetting] = useState<SortSetting>();

  if (!spanMetadataRows) {
    return <EmptyTable title="No spans to show" icon={<Nodes />} />;
  }

  return (
    <SpanMetadataSortedTable
      data={spanMetadataRows}
      sortSetting={sortSetting}
      onChangeSortSetting={setSortSetting}
      columns={columns}
      rowClass={() => cx("table-row")}
    />
  );
};
