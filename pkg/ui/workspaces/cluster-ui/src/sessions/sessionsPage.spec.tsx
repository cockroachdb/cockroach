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
import { MemoryRouter } from "react-router-dom";
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
    cancelSession: _ => {}, // eslint-disable-line @typescript-eslint/no-unused-vars
    cancelQuery: _ => {}, // eslint-disable-line @typescript-eslint/no-unused-vars
    onSortingChange: () => {},
  };

  const baseColumnCount = 5;

  it("shows the extra actions column by default", () => {
    render(
      <MemoryRouter>
        <SessionsPage {...irrelevantProps} />
      </MemoryRouter>,
    );

    expect(screen.getAllByRole("columnheader")).toHaveLength(
      baseColumnCount + 1,
    );
  });

  it("shows the extra actions column when asked", () => {
    render(
      <MemoryRouter>
        <SessionsPage
          uiConfig={{ showTerminateActions: true }}
          {...irrelevantProps}
        />
        ,
      </MemoryRouter>,
    );

    expect(screen.getAllByRole("columnheader")).toHaveLength(
      baseColumnCount + 1,
    );
  });

  it("hides the extra actions column when asked", () => {
    render(
      <MemoryRouter>
        <SessionsPage
          uiConfig={{ showTerminateActions: false }}
          {...irrelevantProps}
        />
        ,
      </MemoryRouter>,
    );

    expect(screen.getAllByRole("columnheader")).toHaveLength(baseColumnCount);
  });
});
