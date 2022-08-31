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
import { Nodes, Caution, Plus, Minus } from "@cockroachlabs/icons";
import { Span, Snapshot } from "src/api/tracezApi";
import { EmptyTable } from "src/empty";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";

import styles from "../snapshot.module.scss";
import classNames from "classnames/bind";
import { TimestampToMoment } from "src/util";
import { formatDuration } from "src/jobs";
import { Badge } from "src/badge";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import ISpanTag = cockroach.server.serverpb.ISpanTag;
const cx = classNames.bind(styles);

class SpanSortedTable extends SortedTable<Span> {}

interface TagBadgeProps {
  t: ISpanTag;
}
const TagBadge = ({ t }: TagBadgeProps) => {
  let highlight = null;
  if (t.highlight) {
    highlight = <Caution />;
  }
  let arrow = null;
  if (t.inherited) {
    arrow = <span title="from parent">(↓)</span>;
  } else if (t.copied_from_child) {
    arrow = <span title="from child">(↑)</span>;
  }
  return (
    <Badge
      text={
        <>
          {highlight}
          {t.key}
          {arrow}
          {t.val ? ":" + t.val : ""}
        </>
      }
      size="small"
      status="info"
      forceUpperCase={false}
    />
  );
};

const NonExpandableTagCell = (props: { span: Span }) => {
  const nonExpandableTags = props.span.processed_tags.filter(
    tag => !tag?.children?.length,
  );

  return (
    <div className={cx("non-expandable-tag-cell")}>
      {nonExpandableTags.map((t, i) => (
        <TagBadge t={t} key={i} />
      ))}
    </div>
  );
};

const ExpandableTag = (props: { tag: ISpanTag }) => {
  const { tag } = props;
  const [isExpanded, setExpanded] = useState(false);
  const { children } = tag;
  const maxChars = max(
    children.map(childTag => childTag.key.length + childTag.val.length),
  );
  return (
    <div style={{ minWidth: maxChars + 4 + "ch" }}>
      {!isExpanded && (
        <div className={cx("expandable-tag-cell")}>
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
        <div className={cx("expandable-tag-cell")}>
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

const ExpandableTagCell = (props: { span: Span }) => {
  const expandableTags = props.span.processed_tags.filter(
    tag => tag?.children?.length,
  );

  return (
    <div>
      {expandableTags.map((t, i) => (
        <ExpandableTag tag={t} key={i} />
      ))}
    </div>
  );
};

const makeColumns = (snapshot: Snapshot): ColumnDescriptor<Span>[] => {
  const startTime = TimestampToMoment(snapshot.captured_at);
  return [
    {
      name: "span",
      title: "Span",
      titleAlign: "right",
      cell: span => span.operation,
      sort: span => span.operation,
      hideTitleUnderline: true,
    },
    {
      name: "start time",
      title: "Start Time (UTC)",
      titleAlign: "right",
      cell: span => TimestampToMoment(span.start).calendar(),
      sort: span => TimestampToMoment(span.start),
      hideTitleUnderline: true,
    },
    {
      name: "duration",
      title: "Duration",
      titleAlign: "right",
      cell: span =>
        formatDuration(
          moment.duration(startTime.diff(TimestampToMoment(span.start))),
        ),
      sort: span => startTime.diff(TimestampToMoment(span.start)),
      hideTitleUnderline: true,
    },
    {
      name: "expandableTags",
      title: "Expandable Tags",
      titleAlign: "right",
      cell: span => <ExpandableTagCell span={span} />,
      hideTitleUnderline: true,
    },
    {
      name: "tags",
      title: "Tags",
      titleAlign: "right",
      cell: span => <NonExpandableTagCell span={span} />,
      hideTitleUnderline: true,
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
