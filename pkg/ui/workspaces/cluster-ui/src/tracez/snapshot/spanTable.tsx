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
import { max } from "lodash";
import React, { useState } from "react";
import { Nodes, Caution, Plus, Minus, Eye, Pencil } from "@cockroachlabs/icons";
import { Span, Snapshot } from "src/api/tracezApi";
import { EmptyTable } from "src/empty";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";

import styles from "../snapshot.module.scss";
import classNames from "classnames/bind";
import { TimestampToMoment } from "src/util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import ISpanTag = cockroach.server.serverpb.ISpanTag;
import RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;
const cx = classNames.bind(styles);

class SpanSortedTable extends SortedTable<Span> {}

interface TagRowProps {
  t: ISpanTag;
}
const TagRow = ({ t }: TagRowProps) => {
  let highlight = null;
  if (t.highlight) {
    highlight = <Caution />;
  }
  let arrow = null;
  if (t.inherited) {
    arrow = " ↓";
  } else if (t.copied_from_child) {
    arrow = " ↑";
  }
  return (
    <tr>
      <td>
        {highlight}
        {t.key}
        {arrow}
        {t.val && ":"}
        &nbsp;
      </td>
      <td>{t.val}</td>
    </tr>
  );
};

const TagCell = (props: { span: Span }) => {
  const tags = props.span.processed_tags.filter(tag => !tag?.children?.length);
  const maxChars = max(tags.map(tag => tag.key.length + tag.val.length));
  return (
    <table style={{ minWidth: maxChars + 4 + "ch" }} className={cx("tag-cell")}>
      <tbody>
        {tags.map((t, i) => (
          <TagRow t={t} key={i} />
        ))}
      </tbody>
    </table>
  );
};

const TagGroup = (props: { tag: ISpanTag }) => {
  const { tag } = props;
  const [isExpanded, setExpanded] = useState(false);
  const { children } = tag;
  const maxChars = max(
    children.map(childTag => childTag.key.length + childTag.val.length),
  );
  return (
    <div style={{ minWidth: maxChars + 4 + "ch" }}>
      {!isExpanded && (
        <div className={cx("tag-group-cell")}>
          <Plus
            className={cx("plus-minus")}
            onClick={() => {
              setExpanded(!isExpanded);
            }}
          />
          <div>
            &nbsp;
            {tag.key + " (" + tag.children.length + ")"}
          </div>
        </div>
      )}
      {isExpanded && (
        <div className={cx("tag-group-cell")}>
          <Minus
            className={cx("plus-minus")}
            onClick={() => {
              setExpanded(!isExpanded);
            }}
          />
          <div>
            &nbsp;
            {tag.key + " (" + tag.children.length + ")"}
            <table>
              <tbody>
                {children.map((child, i) => (
                  <tr key={i}>
                    <td>
                      <strong>{child.key + ":"}</strong>&nbsp;
                    </td>
                    <td>{child.val}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

const TagGroupCell = (props: { span: Span }) => {
  const tagGroups = props.span.processed_tags.filter(
    tag => tag?.children?.length,
  );

  return (
    <div>
      {tagGroups.map((t, i) => (
        <TagGroup tag={t} key={i} />
      ))}
    </div>
  );
};

const formatDuration = (d: moment.Duration): string => {
  const hours = Math.floor(d.asHours());
  const hourStr = hours ? hours + "h " : "";
  const minutes = d.minutes();
  let minuteStr = "";
  if (hourStr) {
    minuteStr = minutes.toFixed(0).padStart(2, "0") + "m ";
  } else if (minutes) {
    minuteStr = minutes + "m ";
  }
  const secondPadNum = hourStr || minuteStr ? 2 : 1;
  const secondStr = d.seconds().toFixed(0).padStart(secondPadNum, "0");
  const msStr = "." + d.milliseconds().toFixed(0).padEnd(3, "0") + "s";
  return hourStr + minuteStr + secondStr + msStr;
};
const makeColumns = (snapshot: Snapshot): ColumnDescriptor<Span>[] => {
  const startTime = TimestampToMoment(snapshot.captured_at);
  return [
    {
      name: "icons",
      title: "",
      cell: span => {
        return (
          <>
            {span.current && <Eye />}
            {span.current_recording_mode != RecordingMode.OFF && <Pencil />}
          </>
        );
      },
      className: cx("icon-cell"),
    },
    {
      name: "span",
      title: "Span",
      titleAlign: "left",
      cell: span => span.operation,
      sort: span => span.operation,
      hideTitleUnderline: true,
      className: cx("operation-cell"),
    },
    {
      name: "start time",
      title: "Start Time (UTC)",
      titleAlign: "right",
      cell: span => TimestampToMoment(span.start).format("YYYY-MM-DD HH:mm:ss"),
      sort: span => TimestampToMoment(span.start),
      hideTitleUnderline: true,
      className: cx("table-cell"),
    },
    {
      name: "duration",
      title: "Duration",
      titleAlign: "right",
      cell: span => {
        return formatDuration(
          moment.duration(startTime.diff(TimestampToMoment(span.start))),
        );
      },
      sort: span => startTime.diff(TimestampToMoment(span.start)),
      hideTitleUnderline: true,
      className: cx("table-cell-duration"),
    },
    {
      name: "tagGroups",
      title: "Expandable Tags",
      titleAlign: "left",
      cell: span => <TagGroupCell span={span} />,
      hideTitleUnderline: true,
      className: cx("table-cell"),
    },
    {
      name: "tags",
      title: "Tags",
      titleAlign: "left",
      cell: span => <TagCell span={span} />,
      hideTitleUnderline: true,
      className: cx("table-cell"),
    },
  ];
};

export interface SpanTableProps {
  sort: SortSetting;
  setSort: (value: SortSetting) => void;
  snapshot: Snapshot;
}

export const SpanTable: React.FC<SpanTableProps> = props => {
  const { snapshot, sort, setSort } = props;
  const spans = snapshot?.spans;

  if (!spans) {
    return (
      <EmptyTable
        title="No spans to show"
        icon={<Nodes />}
        message="Spans provide debug information."
      />
    );
  }

  return (
    <SpanSortedTable
      data={spans}
      sortSetting={sort}
      onChangeSortSetting={setSort}
      className={cx("snapshots-table")}
      columns={makeColumns(snapshot)}
      rowClass={() => cx("table-row")}
    />
  );
};
