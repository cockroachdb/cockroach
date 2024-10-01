// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Switch } from "antd";
import classNames from "classnames/bind";
import Long from "long";
import moment from "moment-timezone";
import React, { useCallback, useMemo, useState } from "react";
import { Helmet } from "react-helmet";
import { useHistory } from "react-router-dom";

import {
  GetTracingSnapshotResponse,
  SetTraceRecordingTypeResponse,
  Span,
} from "src/api";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { CircleFilled } from "src/icon";
import { Loading } from "src/loading";
import { SortSetting } from "src/sortedtable";
import { TimestampToMoment } from "src/util";

import styles from "../snapshot.module.scss";

import { SpanMetadataTable } from "./spanMetadataTable";
import { SpanTable, formatDurationHours, TagCell } from "./spanTable";

import RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;

const cx = classNames.bind(styles);

const SpanStatus: React.FC<{
  span: Span;
  setTraceRecordingType: (
    nodeID: string,
    traceID: Long,
    mode: RecordingMode,
  ) => Promise<SetTraceRecordingTypeResponse>;
  nodeID: string;
}> = props => {
  const { span, setTraceRecordingType, nodeID } = props;
  const spanID = span?.span_id;
  const [recordingInFlight, setRecordingInFlight] = useState(false);
  const traceID = span?.trace_id;
  const recording = span?.current_recording_mode !== RecordingMode.OFF;

  const toggleRecording = useCallback(() => {
    setRecordingInFlight(true);
    const targetState = recording ? RecordingMode.OFF : RecordingMode.VERBOSE;
    const resp = setTraceRecordingType(nodeID, traceID, targetState);
    resp
      .then(() => {
        span.current_recording_mode = targetState;
      })
      .finally(() => {
        setRecordingInFlight(false);
      });
    // Objects in hook dependencies use referential equality, not value
    // equality. To force a hook refresh on span, explicitly mark spanID. But,
    // having done that, there's no need to include the span, as it's purely a
    // function of that input.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    nodeID,
    spanID,
    traceID,
    recording,
    setTraceRecordingType,
    setRecordingInFlight,
  ]);

  if (!span?.current) {
    return (
      <div className={cx("span-snapshot-key-value")}>
        <div className={cx("span-snapshot-key")}>Status</div>
        {/* Use a viewbox to shrink the icon just a bit while leaving it centered.*/}
        <CircleFilled className={cx("icon-gray")} viewBox={"-1 -1 12 12"} />
        &nbsp; Inactive
      </div>
    );
  }

  return (
    <>
      <div className={cx("span-snapshot-key-value")}>
        <div className={cx("span-snapshot-key")}>Status</div>
        {/* Use a viewbox to shrink the icon just a bit while leaving it centered.*/}
        <CircleFilled
          className={recording ? cx("icon-red") : cx("icon-green")}
          viewBox={"-1 -1 12 12"}
        />
        &nbsp; Active{recording && ", Recording"}
      </div>
      <div className={cx("span-snapshot-key-value")}>
        <div className={cx("span-snapshot-key")}>Recording</div>
        <Switch
          checkedChildren={"On"}
          unCheckedChildren={"Off"}
          checked={recording}
          disabled={recordingInFlight}
          onClick={toggleRecording}
        />
      </div>
    </>
  );
};

export const SpanComponent: React.FC<{
  snapshot: GetTracingSnapshotResponse;
  spanDetailsURL: (_: Long) => string;
  span: Span;

  rawTraceURL: (_: Long) => string;

  snapshotError: Error;
  snapshotLoading: boolean;
  setTraceRecordingType: (
    nodeID: string,
    traceID: Long,
    recordingMode: RecordingMode,
  ) => Promise<SetTraceRecordingTypeResponse>;
  nodeID: string;
}> = props => {
  const {
    snapshot,
    span,
    spanDetailsURL,
    snapshotError,
    snapshotLoading,
    nodeID,
    rawTraceURL,
    setTraceRecordingType,
  } = props;
  const snapshotID = snapshot?.snapshot.snapshot_id;
  const spans = snapshot?.snapshot.spans;
  const spanID = span?.span_id;

  const childFilteredSnapshot = useMemo(() => {
    return {
      ...snapshot?.snapshot,
      spans: spans?.filter(s => s.parent_span_id.equals(spanID)),
    };
    // Objects in hook dependencies use referential equality, not value
    // equality. To force a hook refresh, explicitly mark nodeID, snapshotID,
    // and spanID. But, having done that, there's no need to include the
    // snapshot and spans, as they're purely a function of those inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeID, snapshotID, spanID]);

  const parentFilteredSnapshot = useMemo(() => {
    return {
      ...snapshot?.snapshot,
      spans: spans?.filter(s => s.span_id.equals(span.parent_span_id)),
    };
    // Objects in hook dependencies use referential equality, not value
    // equality. To force a hook refresh, explicitly mark nodeID, snapshotID,
    // and spanID. But, having done that, there's no need to include the
    // snapshot and spans, as they're purely a function of those inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeID, snapshotID, spanID]);

  const snapshotTime = useMemo(() => {
    return TimestampToMoment(snapshot?.snapshot.captured_at);
    // Objects in hook dependencies use referential equality, not value
    // equality. To force a hook refresh, explicitly mark nodeID and
    // snapshotID. But, having done that, there's no need to include the
    // snapshot, as it's purely a function of those inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeID, snapshotID]);

  const startTime = useMemo(
    () => {
      return TimestampToMoment(span?.start);
    },
    // Objects in hook dependencies use referential equality, not value
    // equality. To force a hook refresh, explicitly mark nodeID, snapshotID
    // and spanID. But, having done that, there's no need to include the
    // span, as it's purely a function of those inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nodeID, snapshotID, spanID],
  );
  const [childSpanSortSetting, setChildSpanSortSetting] =
    useState<SortSetting>();

  const childrenMetadata = span?.children_metadata;

  const history = useHistory();
  return (
    <div className={cx("snapshots-page")}>
      <Helmet title={"Span " + spanID?.toString()} />
      <div
        data-testid="span-component-title"
        className={cx("span-header-columns")}
      >
        <h3 className={cx("span-details-title")}>{span?.operation}</h3>
        <Button
          className={cx("span-details-raw-trace-button")}
          type={"secondary"}
          onClick={() => {
            history.push(rawTraceURL(span.span_id));
          }}
        >
          View Raw Trace
        </Button>
      </div>

      <Loading
        loading={snapshotLoading}
        page={"snapshots"}
        error={snapshotError}
        render={() => (
          <div>
            <section className={cx("span-section", "span-snapshot-columns")}>
              <div className={cx("span-snapshot-column")}>
                <div className={cx("span-snapshot-key-value")}>
                  <div className={cx("span-snapshot-key")}>
                    Snapshot Time (UTC)
                  </div>
                  {snapshotTime.format("YYYY-MM-DD HH:mm:ss.SSS")}
                </div>
                <div className={cx("span-snapshot-key-value")}>
                  <div className={cx("span-snapshot-key")}>
                    Start Time (UTC)
                  </div>
                  {startTime.format("YYYY-MM-DD HH:mm:ss.SSS")}
                </div>
                <div className={cx("span-snapshot-key-value")}>
                  <div className={cx("span-snapshot-key")}>Duration</div>
                  {formatDurationHours(
                    moment.duration(snapshotTime.diff(startTime)),
                  )}
                </div>
                <SpanStatus
                  span={span}
                  setTraceRecordingType={setTraceRecordingType}
                  nodeID={nodeID}
                />
              </div>
              <div>
                <div
                  className={cx("span-snapshot-key", "span-snapshot-key-value")}
                >
                  Tags
                </div>
                {span && <TagCell span={span} defaultExpanded={true} />}
              </div>
            </section>
            {parentFilteredSnapshot.spans?.length > 0 && (
              <section className={cx("span-section")}>
                <h3 className={commonStyles("base-heading")}>Parent Span</h3>
                <SpanTable
                  snapshot={parentFilteredSnapshot}
                  spanDetailsURL={spanDetailsURL}
                />
              </section>
            )}
            {childrenMetadata?.length > 0 && (
              <section className={cx("span-section")}>
                <h3 className={commonStyles("base-heading")}>
                  Aggregated Child Span Metadata
                </h3>

                <SpanMetadataTable childrenMetadata={childrenMetadata} />
              </section>
            )}
            {childFilteredSnapshot.spans?.length > 0 && (
              <section className={cx("span-section")}>
                <h3 className={commonStyles("base-heading")}>Child Spans</h3>
                <SpanTable
                  snapshot={childFilteredSnapshot}
                  setSort={setChildSpanSortSetting}
                  sort={childSpanSortSetting}
                  spanDetailsURL={spanDetailsURL}
                />
              </section>
            )}
          </div>
        )}
      />
      <div className={cx("bottom-padding")} />
    </div>
  );
};
