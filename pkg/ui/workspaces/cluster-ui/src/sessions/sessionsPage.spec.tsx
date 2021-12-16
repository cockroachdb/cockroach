// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { createMemoryHistory } from "history";
import { render, screen } from "@testing-library/react";

import SessionsPage, { SessionsPageProps } from "./sessionsPage";

describe("SessionsPage", () => {
  const irrelevantProps: SessionsPageProps = {
    history: createMemoryHistory({ initialEntries: ["/sessions"] }),
    location: { pathname: "/sessions", search: "", hash: "", state: null },
    match: { path: "/sessions", url: "/sessions", isExact: true, params: {} },
    sessions: [],
    sessionsError: null,
    sortSetting: { ascending: false, columnTitle: "statementAge" },
    refreshSessions: () => {},
    cancelSession: _ => {},
    cancelQuery: _ => {},
    onSortingChange: () => {},
  };

  const baseColumnCount = 5;

  it("shows the extra actions column by default", () => {
    render(<SessionsPage {...irrelevantProps} />);

    expect(screen.getAllByRole("columnheader")).toHaveLength(
      baseColumnCount + 1,
    );
  });

  it("shows the extra actions column when asked", () => {
    render(
      <SessionsPage
        uiConfig={{ showTerminateActions: true }}
        {...irrelevantProps}
      />,
    );

    expect(screen.getAllByRole("columnheader")).toHaveLength(
      baseColumnCount + 1,
    );
  });

  it("hides the extra actions column when asked", () => {
    render(
      <SessionsPage
        uiConfig={{ showTerminateActions: false }}
        {...irrelevantProps}
      />,
    );

    expect(screen.getAllByRole("columnheader")).toHaveLength(baseColumnCount);
  });
});
