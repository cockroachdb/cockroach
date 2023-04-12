// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ROUTE_PREFIX, SnapshotPage, SnapshotPageProps } from "./snapshotPage";
import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import * as H from "history";

import { SortSetting } from "../../sortedtable";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import {
  RecordingMode,
  SetTraceRecordingTypeResponse,
  TakeTracingSnapshotResponse,
} from "src/api/tracezApi";
import GetTracingSnapshotResponse = cockroach.server.serverpb.GetTracingSnapshotResponse;
import Long from "long";
import { getByTestId } from "@testing-library/dom/types/queries";

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
    refreshRawTrace: (req: {
      nodeID: string;
      snapshotID: number;
      traceID: Long;
    }) => void {},
    refreshSnapshot: (req: { nodeID: string; snapshotID: number }): void => {},
    refreshSnapshots: (id: string): void => {},
    setSort: (value: SortSetting): void => {},
    setTraceRecordingType: (
      nodeID: string,
      traceID: Long,
      recordingMode: RecordingMode,
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
    takeSnapshot: (nodeID: string): Promise<TakeTracingSnapshotResponse> => {
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
