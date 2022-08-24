// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React, { useEffect, useState } from "react";
import _ from "lodash";
import Long from "long";
import {
  Badge,
  BadgeStatus,
  ColumnDescriptor,
  EmptyTable,
  Search,
  SortedTable,
  SortSetting,
  util,
} from "@cockroachlabs/cluster-ui";
import "./tracez.styl";
import { Button, Icon } from "@cockroachlabs/ui-components";
import Dropdown from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import { cockroach, google } from "src/js/protos";
import {
  getLiveTrace,
  getTraceForSnapshot,
  getTracingSnapshot,
  listTracingSnapshots,
  setTraceRecordingType,
  takeTracingSnapshot,
} from "src/util/api";
import { CaretRight } from "@cockroachlabs/icons";
import { Switch } from "antd";
import "antd/lib/switch/style";
import ISnapshotInfo = cockroach.server.serverpb.ISnapshotInfo;
import ITracingSpan = cockroach.server.serverpb.ITracingSpan;
import GetTracingSnapshotRequest = cockroach.server.serverpb.GetTracingSnapshotRequest;
import GetTraceRequest = cockroach.server.serverpb.GetTraceRequest;
import IGetTraceResponse = cockroach.server.serverpb.IGetTraceResponse;
import ISpanTag = cockroach.server.serverpb.ISpanTag;
import SetTraceRecordingTypeRequest = cockroach.server.serverpb.SetTraceRecordingTypeRequest;
import RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;

const TS_FORMAT = "MMMM Do YYYY, H:mm:ss"; // January 28th 2022, 19:12:40;

const tsToFormat = (ts: google.protobuf.ITimestamp) =>
  util.TimestampToMoment(ts).format(TS_FORMAT);

const SnapshotSelector = ({
  setSnapshot,
  snapshots,
  currentSnapshot,
}: {
  setSnapshot: (id: Long) => void;
  snapshots: ISnapshotInfo[];
  currentSnapshot: Snapshot;
}) => {
  return (
    <Dropdown
      title="Snapshots"
      options={snapshots.map(s => {
        return {
          value: `${s.snapshot_id}`,
          label: `${s.snapshot_id}: ${tsToFormat(s.captured_at)}`,
        };
      })}
      selected={`${currentSnapshot.id}`}
      onChange={dropdownOption =>
        setSnapshot(Long.fromString(dropdownOption.value))
      }
    />
  );
};

interface SnapshotRow {
  span: ITracingSpan;
  stack: string;
}

const GoroutineToggler = ({ id, stack }: { id: Long; stack: string }) => {
  const [showStack, setShowStack] = useState<Boolean>(false);

  if (!showStack) {
    return (
      <Button as="a" intent="tertiary" onClick={() => setShowStack(true)}>
        {id.toString(10)}
      </Button>
    );
  } else {
    return (
      <div>
        <Button as="a" intent="tertiary" onClick={() => setShowStack(false)}>
          Hide
        </Button>
        <pre>{stack}</pre>
      </div>
    );
  }
};

interface TagValueProps {
  t: ISpanTag;
  setSearch: (s: string) => void;
}

const TagValue = ({ t, setSearch }: TagValueProps) => {
  let v = <>{t.val}</>;
  if (t.link) {
    v = (
      <Button as="a" onClick={() => setSearch(t.link)}>
        {t.val}
      </Button>
    );
  }
  return <span title={t.caption}>{v}</span>;
};

interface TagBadgeProps {
  t: ISpanTag;
  setSearch: (s: string) => void;
  toggleExpanded?: () => void;
  isExpanded: Boolean;
  status?: BadgeStatus;
}

const TagBadge = ({
  t,
  setSearch,
  toggleExpanded,
  isExpanded,
  status,
}: TagBadgeProps) => {
  let highlight = null;
  if (t.highlight) {
    highlight = <Icon iconName="Caution" />;
  }
  let arrow = null;
  if (t.inherited) {
    arrow = <span title="from parent">(↓)</span>;
  } else if (t.copied_from_child) {
    arrow = <span title="from child">(↑)</span>;
  }
  const isExpandable = Boolean(t.children && t.children.length);

  const icon = !isExpandable ? null : isExpanded ? (
    <Icon iconName={"CaretDown"} />
  ) : (
    <Icon iconName={"CaretRight"} />
  );

  let badgeStatus: BadgeStatus;
  if (status) {
    badgeStatus = status;
  } else if (t.hidden) {
    badgeStatus = "default";
  } else if (isExpandable) {
    badgeStatus = "warning";
  } else {
    badgeStatus = "info";
  }
  return (
    <Button
      className="tag-button"
      intent="tertiary"
      onClick={() => {
        if (!isExpandable) {
          return;
        }
        toggleExpanded();
      }}
    >
      <Badge
        text={
          <>
            {highlight}
            {t.key}
            {arrow}
            {t.val ? ":" : ""}
            <TagValue t={t} setSearch={setSearch} />
          </>
        }
        size="small"
        status={badgeStatus}
        icon={icon}
        forceUpperCase={false}
      />
    </Button>
  );
};

const OperationCell = (props: {
  sr: SnapshotRow;
  setRecording: (trace: cockroach.server.serverpb.ITracingSpan) => void;
}) => {
  return (
    <div>
      <Button
        as="a"
        intent="tertiary"
        onClick={() => props.setRecording(props.sr.span)}
      >
        {props.sr.span.operation}
      </Button>
    </div>
  );
};

const TagCell = (props: {
  sr: SnapshotRow;
  setSearch: (s: string) => void;
}) => {
  const [expandedTagIndex, setExpandedTagIndex] = useState<number>(-1);
  const processedTags = props.sr.span.processed_tags;

  // Pad 8px on top and bottom.
  //
  // Table rows have a minimum height of 70px, and this is not configurable.
  //
  // With this particular badge styling, that gives the illusion of 15 pixels
  // of padding, with the TagBadge centered vertically. But this implicit
  // padding will disappear when a cell is expanded, causing the top series of
  // TagBadges to move up 8 pixels. This is disorienting, so avoid it by
  // making the padding official.
  return (
    <div className={"outer-row"}>
      <div className={"inner-row"}>
        {processedTags.map((t, i) => (
          <TagBadge
            t={t}
            setSearch={props.setSearch}
            isExpanded={expandedTagIndex == i}
            toggleExpanded={() => {
              if (expandedTagIndex == i) {
                setExpandedTagIndex(-1);
              } else {
                setExpandedTagIndex(i);
              }
            }}
            key={i}
          />
        ))}
      </div>
      {expandedTagIndex != -1 && (
        <div className={"inner-row"}>
          {processedTags[expandedTagIndex].children.map((t, i) => (
            <TagBadge
              t={t}
              key={i}
              status={
                processedTags[expandedTagIndex].hidden ? "default" : "warning"
              }
              setSearch={props.setSearch}
              isExpanded={false}
            />
          ))}
        </div>
      )}
    </div>
  );
};

const snapshotColumns = (
  setRecording: (span: ITracingSpan) => void,
  setSearch: (s: string) => void,
  setTraceRecordingVerbose: (span: ITracingSpan) => void,
): ColumnDescriptor<SnapshotRow>[] => {
  return [
    {
      title: "Operation",
      name: "operation",
      cell: sr => <OperationCell sr={sr} setRecording={setRecording} />,
      sort: sr => sr.span.operation,
    },
    {
      title: "Tags",
      name: "tags",
      cell: sr => <TagCell sr={sr} setSearch={setSearch} />,
    },
    {
      title: "Recording",
      name: "recording",
      cell: sr => (
        <Switch
          disabled={!sr.span.current}
          checked={sr.span.current_recording_mode != RecordingMode.OFF}
          onClick={() => setTraceRecordingVerbose(sr.span)}
        />
      ),
      sort: sr => `${sr.span.current_recording_mode}`,
    },
    {
      title: "Start Time",
      name: "startTime",
      cell: sr => tsToFormat(sr.span.start),
      sort: sr => util.TimestampToMoment(sr.span.start),
    },
    {
      title: "Goroutine ID",
      name: "goroutineID",
      cell: sr => (
        <GoroutineToggler id={sr.span.goroutine_id} stack={sr.stack} />
      ),
      sort: sr => sr.span.goroutine_id.toNumber(),
    },
  ];
};

export class SnapshotSortedTable extends SortedTable<SnapshotRow> {}

const CurrentSnapshot = ({
  snapshot,
  search,
  setRecording,
  setSearch,
  setTraceRecordingVerbose,
}: {
  snapshot: Snapshot;
  search: string;
  setRecording: (trace: cockroach.server.serverpb.ITracingSpan) => void;
  setSearch: (s: string) => void;
  setTraceRecordingVerbose: (span: ITracingSpan) => void;
}) => {
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "startTime",
  });
  return (
    <SnapshotSortedTable
      data={snapshot.rows.filter(r => {
        return JSON.stringify(r).toLowerCase().includes(search.toLowerCase());
      })}
      columns={snapshotColumns(
        setRecording,
        setSearch,
        setTraceRecordingVerbose,
      )}
      sortSetting={sortSetting}
      onChangeSortSetting={setSortSetting}
      renderNoResult={<EmptyTable title="No snapshot selected" />}
    />
  );
};

interface Snapshot {
  id?: Long;
  rows?: SnapshotRow[];
  captured_at?: google.protobuf.ITimestamp;
}

export const Tracez = () => {
  // Snapshot state
  const [snapshot, setSnapshot] = useState<Snapshot>({ rows: [] });
  const [search, setSearch] = useState<string>("");
  const [snapshots, setSnapshots] = useState<ISnapshotInfo[]>([]);

  // Recording view state
  // In the UI when you click on an operation we set the requestedSpan. Then
  // the effect is triggered to retrieve the trace for that span, once that's
  // updated the UI is changed.
  const [requestedSpan, setRequestedSpan] =
    useState<cockroach.server.serverpb.ITracingSpan>(null);
  const [currentTrace, setCurrentTrace] = useState<IGetTraceResponse>(null);
  const [showTrace, setShowTrace] = useState<boolean>(false);
  const [showLiveTrace, setShowLiveTrace] = useState<boolean>(false);

  const setSnapshotID = (id: Long) => {
    const req = new GetTracingSnapshotRequest({
      snapshot_id: id,
    });
    getTracingSnapshot(req).then(req => {
      setSnapshot({
        id: req.snapshot.snapshot_id,
        captured_at: req.snapshot.captured_at,
        rows: req.snapshot.spans.map(
          (span: cockroach.server.serverpb.ITracingSpan): SnapshotRow => {
            return {
              span,
              stack: req.snapshot.stacks[`${span.goroutine_id}`],
            };
          },
        ),
      });
    });
  };

  // takeSnapshot takes a snapshot and displays it.
  const takeSnapshot = () => {
    takeTracingSnapshot().then(resp => {
      refreshTracingSnapshots();
      // Load the new snapshot.
      setSnapshotID(resp.snapshot.snapshot_id);
    });
  };

  const refreshTracingSnapshots = () => {
    listTracingSnapshots().then(resp => {
      setSnapshots(resp.snapshots);
    });
  };

  useEffect(refreshTracingSnapshots, []);

  useEffect(() => {
    if (showTrace) {
      if (showLiveTrace) {
        getLiveTrace(
          new GetTraceRequest({
            trace_id: requestedSpan.trace_id,
            recording_type: RecordingMode.VERBOSE,
          }),
        ).then(resp => {
          setCurrentTrace(resp);
          setShowTrace(true);
        });
      } else {
        getTraceForSnapshot(
          new GetTraceRequest({
            trace_id: requestedSpan.trace_id,
            snapshot_id: snapshot.id,
          }),
        ).then(resp => {
          setCurrentTrace(resp);
          setShowTrace(true);
        });
      }
    }
  }, [showTrace, snapshot, requestedSpan, showLiveTrace]);

  const setTraceRecordingVerbose = (span: ITracingSpan) => {
    const recMode =
      span.current_recording_mode != RecordingMode.OFF
        ? RecordingMode.OFF
        : RecordingMode.VERBOSE;
    setTraceRecordingType(
      new SetTraceRecordingTypeRequest({
        trace_id: span.trace_id,
        recording_mode: recMode,
      }),
    ).then(() => {
      // We modify the snapshot in place.
      setSnapshot({
        id: snapshot.id,
        captured_at: snapshot.captured_at,
        rows: snapshot.rows.map(r => {
          if (r.span.trace_id === span.trace_id) {
            r.span.current_recording_mode = recMode;
          }
          return r;
        }),
      });
    });
  };
  return (
    <div>
      {showTrace && currentTrace ? (
        <TraceView
          currentTrace={currentTrace}
          cancel={() => {
            setShowTrace(false);
            setShowLiveTrace(false);
          }}
          showLive={() => {
            setShowLiveTrace(true);
          }}
          operation={requestedSpan.operation}
        />
      ) : (
        <SnapshotView
          takeSnapshot={takeSnapshot}
          setSnapshotID={setSnapshotID}
          snapshots={snapshots}
          snapshot={snapshot}
          setSearch={setSearch}
          search={search}
          setRecording={span => {
            setRequestedSpan(span);
            setShowTrace(true);
          }}
          setTraceRecordingVerbose={setTraceRecordingVerbose}
        />
      )}
    </div>
  );
};

interface TraceViewProps {
  currentTrace: IGetTraceResponse;
  cancel: () => void;
  showLive: () => void;
  operation: string;
}

const TraceView = ({
  currentTrace,
  cancel,
  showLive,
  operation,
}: TraceViewProps) => {
  return (
    <>
      <h3 className="base-heading">
        Active Traces{" "}
        <span>
          <CaretRight />
          {currentTrace.snapshot_id.toNumber() === 0
            ? "Latest"
            : `Snapshot: ${currentTrace.snapshot_id}`}
          <CaretRight />
          {operation}
        </span>
      </h3>
      <PageConfig>
        <PageConfigItem>
          <Button as={"button"} onClick={cancel}>
            Back
          </Button>
        </PageConfigItem>
        <PageConfigItem>
          <Button as={"button"} onClick={showLive}>
            Switch to Latest
          </Button>
        </PageConfigItem>
      </PageConfig>
      <section className="section" style={{ maxWidth: "none" }}>
        <pre>{currentTrace.serialized_recording}</pre>
      </section>
    </>
  );
};

interface SnapshotViewProps {
  takeSnapshot: () => void;
  setSnapshotID: (s: Long) => void;
  snapshots: ISnapshotInfo[];
  snapshot: Snapshot;
  setSearch: (s: string) => void;
  search: string;
  setRecording: (s: cockroach.server.serverpb.ITracingSpan) => void;
  setTraceRecordingVerbose: (span: ITracingSpan) => void;
}

const SnapshotView = ({
  takeSnapshot,
  setSnapshotID,
  snapshots,
  snapshot,
  setSearch,
  search,
  setRecording,
  setTraceRecordingVerbose,
}: SnapshotViewProps) => (
  <>
    <h3 className="base-heading">
      Active Traces
      {snapshot.id ? (
        <>
          <CaretRight />
          {`Snapshot: ${snapshot.id}`}
        </>
      ) : null}
    </h3>
    <PageConfig>
      <PageConfigItem>
        <Button onClick={takeSnapshot} intent="secondary">
          <Icon iconName="Download" /> Take snapshot
        </Button>
      </PageConfigItem>
      <PageConfigItem>
        <SnapshotSelector
          setSnapshot={setSnapshotID}
          snapshots={snapshots || []}
          currentSnapshot={snapshot}
        />
      </PageConfigItem>
      <PageConfigItem>
        <Search
          /* Use of `any` type here is due to some issues with `Search` component. */
          onSubmit={setSearch as any}
          onClear={() => setSearch("")}
          defaultValue={search}
          placeholder={"Search snapshot"}
        />
      </PageConfigItem>
    </PageConfig>
    <section className="section">
      <CurrentSnapshot
        snapshot={snapshot}
        search={search}
        setRecording={setRecording}
        setSearch={setSearch}
        setTraceRecordingVerbose={setTraceRecordingVerbose}
      />
    </section>
  </>
);
