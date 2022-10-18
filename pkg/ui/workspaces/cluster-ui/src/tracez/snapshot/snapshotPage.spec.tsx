// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { SnapshotPage, SnapshotPageProps } from "./snapshotPage";
import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import * as H from "history";

import { SortSetting } from "../../sortedtable";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { TakeTracingSnapshotResponse } from "src/api/tracezApi";
import GetTracingSnapshotResponse = cockroach.server.serverpb.GetTracingSnapshotResponse;

const getMockSnapshotPageProps = (): SnapshotPageProps => {
  const history = H.createHashHistory();
  return {
    snapshotsValid: false,
    location: history.location,
    history,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params: {},
    },
    refreshSnapshot: (_req: { nodeID: string; snapshotID: number }): void => {},
    refreshSnapshots: (_nodeID: string): void => {},
    refreshNodes: (): void => {},
    takeSnapshot(_nodeID: string): Promise<TakeTracingSnapshotResponse> {
      return Promise.resolve(undefined);
    },
    setSort: (value: SortSetting): void => {},
    snapshotError: undefined,
    snapshotLoading: false,
    snapshots: undefined,
    snapshotsError: undefined,
    snapshotsLoading: false,
    sort: undefined,
    snapshot: null,
  };
};

describe("Snapshot", () => {
  it("renders expected snapshot table columns", () => {
    const props = getMockSnapshotPageProps();
    props.match.params.snapshotID = "1";
    props.snapshot = GetTracingSnapshotResponse.fromObject({
      snapshot: {
        spans: [{ span_id: 1 }],
      },
    });
    const { getAllByText } = render(
      <MemoryRouter>
        <SnapshotPage {...props} />
      </MemoryRouter>,
    );
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
});
