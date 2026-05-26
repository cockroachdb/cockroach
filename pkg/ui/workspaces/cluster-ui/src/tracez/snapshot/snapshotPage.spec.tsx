// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { render } from "@testing-library/react";
import * as H from "history";
import Long from "long";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import {
  RecordingMode,
  SetTraceRecordingTypeResponse,
  TakeTracingSnapshotResponse,
} from "src/api/tracezApi";

import { SortSetting } from "../../sortedtable";

import { ROUTE_PREFIX, SnapshotPage, SnapshotPageProps } from "./snapshotPage";

import GetTracingSnapshotResponse = cockroach.server.serverpb.GetTracingSnapshotResponse;

const getMockSnapshotPageProps = (): SnapshotPageProps => {
  const history = H.createHashHistory();
  history.location.pathname =
    ROUTE_PREFIX + "node/:nodeID/snapshot/:snapshotID/";
  return {
    history,
    location: history.location,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params: {
        nodeID: "1",
        snapshotID: "1",
      },
    },
    rawTrace: undefined,
    rawTraceLoading: false,
    refreshNodes: () => void {},
    refreshRawTrace: () => void {},
    refreshSnapshot: (_req: { nodeID: string; snapshotID: number }): void => {},
    refreshSnapshots: (_id: string): void => {},
    setSort: (_value: SortSetting): void => {},
    setTraceRecordingType: (
      _nodeID: string,
      _traceID: Long,
      _recordingMode: RecordingMode,
    ): Promise<SetTraceRecordingTypeResponse> => {
      return Promise.resolve(undefined);
    },
    snapshot: new GetTracingSnapshotResponse({
      snapshot: {
        spans: [
          new cockroach.server.serverpb.TracingSpan({
            span_id: Long.fromInt(1),
            parent_span_id: Long.fromInt(0),
            operation: "spanny",
          }),
        ],
      },
    }),
    snapshotLoading: false,
    snapshotsLoading: false,
    sort: undefined,
    takeSnapshot: (_nodeID: string): Promise<TakeTracingSnapshotResponse> => {
      return Promise.resolve(undefined);
    },
  };
};

describe("Snapshot", () => {
  it("renders expected snapshot table columns", () => {
    const props = getMockSnapshotPageProps();
    const { getAllByText, getByTestId } = render(
      <MemoryRouter>
        <SnapshotPage {...props} />
      </MemoryRouter>,
    );

    getByTestId("snapshot-component-title");

    const expectedColumnTitles = [
      "Span",
      "Start Time (UTC)",
      "Duration",
      "Tags",
    ];

    for (const columnTitle of expectedColumnTitles) {
      getAllByText(columnTitle);
    }
  });

  it("renders span view", () => {
    const props = getMockSnapshotPageProps();
    props.match.params["spanID"] = "1";
    const { getByTestId } = render(
      <MemoryRouter>
        <SnapshotPage {...props} />
      </MemoryRouter>,
    );

    getByTestId("span-component-title");
  });

  it("renders raw trace view", () => {
    const props = getMockSnapshotPageProps();
    props.match.path =
      ROUTE_PREFIX + "node/:nodeID/snapshot/:snapshotID/span/:spanID/raw";
    props.match.params["spanID"] = "1";

    const { getByTestId } = render(
      <MemoryRouter>
        <SnapshotPage {...props} />
      </MemoryRouter>,
    );
    getByTestId("raw-trace-component");
  });
});
