// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Nodes, Caution, Plus, Minus } from "@cockroachlabs/icons";
import classNames from "classnames/bind";
import Long from "long";
import moment from "moment-timezone";
import React, { useState } from "react";
import { Link } from "react-router-dom";

import { Span, Snapshot } from "src/api/tracezApi";
import { Dropdown } from "src/dropdown";
import { EmptyTable } from "src/empty";
import { CircleFilled } from "src/icon";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { TimestampToMoment } from "src/util";

import styles from "../snapshot.module.scss";

import RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;
import ISpanTag = cockroach.server.serverpb.ISpanTag;
const cx = classNames.bind(styles);

class SpanSortedTable extends SortedTable<Span> {}

interface TagRowProps {
  t: ISpanTag;
}
const Tag = ({ t }: TagRowProps) => {
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
    <div className={cx("tag")}>
      {highlight}
      {t.key}
      {arrow}
      {t.val && ": "}
      {t.val}
    </div>
  );
};

export const TagCell = (props: { span: Span; defaultExpanded?: boolean }) => {
  const { span, defaultExpanded } = props;
  const tagGroups = span.processed_tags.filter(tag => tag?.children?.length);
  const tags = span.processed_tags.filter(tag => !tag?.children?.length);

  let hiddenTagGroup = null;
  if (tagGroups.length && tagGroups[tagGroups.length - 1].key === "...") {
    // This should always be present, and always be last, but wrap it in the check just in case.
    hiddenTagGroup = tagGroups.pop();
  }

  return (
    <div className={cx("tag-cell")}>
      <div className={cx("single-tags-row")}>
        {tags.map((t, i) => (
          <Tag t={t} key={i} />
        ))}
        {hiddenTagGroup && (
          <TagGroup tag={hiddenTagGroup} defaultExpanded={defaultExpanded} />
        )}
      </div>
      {tagGroups.map((t, i) => (
        <TagGroup tag={t} key={i} defaultExpanded={defaultExpanded} />
      ))}
    </div>
  );
};

const TagGroup = (props: { tag: ISpanTag; defaultExpanded?: boolean }) => {
  const { tag, defaultExpanded } = props;
  const [isExpanded, setExpanded] = useState(Boolean(defaultExpanded));
  const { children } = tag;
  return (
    <div className={cx("tag")}>
      {!isExpanded && (
        <div className={cx("tag-group")}>
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
        <div className={cx("tag-group")}>
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

const SpanCell: React.FC<{
  span: Span;
  spanDetailsURL: (spanID: Long) => string;
}> = props => {
  const { span, spanDetailsURL } = props;
  const toPath = spanDetailsURL(span.span_id);
  return <Link to={toPath}>{span.operation}</Link>;
};

export const formatDurationHours = (d: moment.Duration): string => {
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
const makeColumns = (
  snapshot: Snapshot,
  relativeTimeMode: boolean,
  setRelativeTimeMode: (value: boolean) => void,
  spanDetailsURL: (spanID: Long) => string,
  sortable: boolean,
): ColumnDescriptor<Span>[] => {
  const startTime = TimestampToMoment(snapshot.captured_at);

  const timeModes = [
    {
      name: "Start Time (UTC)",
      value: false,
    },
    {
      name: "Duration",
      value: true,
    },
  ];
  return [
    {
      name: "icons",
      title: "",
      cell: span => {
        return span.current_recording_mode !== RecordingMode.OFF ? (
          // Use a viewbox to shrink the icon just a bit while leaving it centered.
          <CircleFilled className={cx("icon-red")} viewBox={"-1 -1 12 12"} />
        ) : span.current ? (
          <CircleFilled className={cx("icon-green")} viewBox={"-1 -1 12 12"} />
        ) : null;
      },
      className: cx("icon-cell"),
    },
    {
      name: "span",
      title: "Span",
      titleAlign: "left",
      cell: span => <SpanCell span={span} spanDetailsURL={spanDetailsURL} />,
      sort: sortable && (span => span.operation),
      hideTitleUnderline: true,
      className: cx("operation-cell"),
    },
    {
      name: "time",
      title: (
        <div
          onClick={e => {
            e.stopPropagation();
          }}
          className={cx("table-title-time")}
        >
          <Dropdown<boolean>
            items={timeModes}
            onChange={value => {
              setRelativeTimeMode(value);
            }}
          >
            {relativeTimeMode ? "Duration" : "Start Time (UTC)"}
          </Dropdown>
        </div>
      ),
      titleAlign: "right",
      cell: span => {
        return relativeTimeMode
          ? formatDurationHours(
              moment.duration(startTime.diff(TimestampToMoment(span.start))),
            )
          : TimestampToMoment(span.start).format("YYYY-MM-DD HH:mm:ss");
      },
      // This sort is backwards in duration mode, but toggling between sort keys within a column really trips things up.
      sort: sortable && (span => TimestampToMoment(span.start).unix()),
      hideTitleUnderline: true,
      className: cx("table-cell-time"),
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
  sort?: SortSetting;
  setSort?: (value: SortSetting) => void;
  snapshot: Snapshot;
  spanDetailsURL: (spanID: Long) => string;
}

export const SpanTable: React.FC<SpanTableProps> = props => {
  const { snapshot, sort, setSort, spanDetailsURL } = props;
  const spans = snapshot?.spans;

  const [relativeTimeMode, setRelativeTimeMode] = useState(false);
  if (!spans) {
    return <EmptyTable title="No spans to show" icon={<Nodes />} />;
  }

  return (
    <SpanSortedTable
      data={spans}
      sortSetting={sort}
      onChangeSortSetting={setSort}
      columns={makeColumns(
        snapshot,
        relativeTimeMode,
        setRelativeTimeMode,
        spanDetailsURL,
        Boolean(setSort),
      )}
      rowClass={() => cx("table-row")}
    />
  );
};
