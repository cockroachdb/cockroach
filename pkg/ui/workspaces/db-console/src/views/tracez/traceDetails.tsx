import { useParams } from "react-router-dom";
import React, { useEffect, useState } from "react";
import Long from "long";
import { cockroach } from "src/js/protos";
import IGetTraceResponse = cockroach.server.serverpb.IGetTraceResponse;
import { getLiveTrace, getTraceForSnapshot } from "oss/src/util/api";
import GetTraceRequest = cockroach.server.serverpb.GetTraceRequest;
import RecordingMode = cockroach.util.tracing.tracingpb.RecordingMode;
import { CaretRight } from "@cockroachlabs/icons";
import {
  PageConfig,
  PageConfigItem,
} from "oss/src/views/shared/components/pageconfig";
import { Button } from "@cockroachlabs/ui-components";

export const TraceDetails = () => {
  // Recording view state
  // In the UI when you click on an operation we set the requestedSpan. Then
  // the effect is triggered to retrieve the trace for that span, once that's
  // updated the UI is changed.
  const [currentTrace, setCurrentTrace] = useState<IGetTraceResponse>(null);
  const [showLiveTrace, setShowLiveTrace] = useState<boolean>(false);

  const { collectionID: collectionIDParam, traceID: traceIDParam } = useParams<{
    collectionID: string;
    traceID: string;
  }>();

  const snapshotID = collectionIDParam
    ? Long.fromString(collectionIDParam)
    : undefined;
  const traceID = traceIDParam ? Long.fromString(traceIDParam) : undefined;

  useEffect(() => {
    if (!snapshotID || !traceID) {
      return;
    }
    if (showLiveTrace) {
      getLiveTrace(
        new GetTraceRequest({
          trace_id: traceID,
          recording_type: RecordingMode.VERBOSE,
        }),
      ).then(resp => {
        setCurrentTrace(resp);
      });
    } else {
      getTraceForSnapshot(
        new GetTraceRequest({
          trace_id: traceID,
          snapshot_id: snapshotID,
        }),
      ).then(resp => {
        setCurrentTrace(resp);
      });
    }
  }, [collectionIDParam, traceIDParam, showLiveTrace, setCurrentTrace]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <TraceView
      currentTrace={currentTrace}
      cancel={() => {
        setShowLiveTrace(false);
        history.back();
      }}
      showLive={() => {
        setShowLiveTrace(true);
      }}
    />
  );
};

interface TraceViewProps {
  currentTrace: IGetTraceResponse;
  cancel: () => void;
  showLive: () => void;
}

const TraceView = ({ currentTrace, cancel, showLive }: TraceViewProps) => {
  if (!currentTrace) {
    return null;
  }
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
          {currentTrace.operation}
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
