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

import SessionDetails, { SessionDetailsProps } from "./sessionDetails";
import { activeSession } from "./sessionsPage.fixture";

describe("SessionDetails", () => {
  const irrelevantProps: SessionDetailsProps = {
    history: createMemoryHistory({ initialEntries: ["/sessions"] }),
    location: {
      pathname: "/sessions/blah",
      search: "",
      hash: "",
      state: null,
    },
    match: {
      path: "/sessions/blah",
      url: "/sessions/blah",
      isExact: true,
      params: { session: "blah" },
    },
    nodeNames: {},
    session: activeSession,
    sessionError: null,
    refreshSessions: () => {},
    refreshNodes: () => {},
    refreshNodesLiveness: () => {},
    cancelSession: _ => {}, // eslint-disable-line @typescript-eslint/no-unused-vars
    cancelQuery: _ => {}, // eslint-disable-line @typescript-eslint/no-unused-vars
  };

  const irrelevantUIConfig: Omit<
    SessionDetailsProps["uiConfig"],
    "showTerminateActions"
  > = {
    showGatewayNodeLink: true,
  };

  it("shows the cancel buttons by default", () => {
    render(
      <MemoryRouter>
        <SessionDetails {...irrelevantProps} />
      </MemoryRouter>,
    );
    expect(screen.queryByText("Cancel session")).not.toBeNull();
  });

  it("shows the cancel buttons when asked", () => {
    render(
      <MemoryRouter>
        <SessionDetails
          {...irrelevantProps}
          uiConfig={{ showTerminateActions: true, ...irrelevantUIConfig }}
        />
        ,
      </MemoryRouter>,
    );
    expect(screen.queryByText("Cancel session")).not.toBeNull();
  });

  it("hides the cancel buttons when asked", () => {
    render(
      <MemoryRouter>
        <SessionDetails
          {...irrelevantProps}
          uiConfig={{ showTerminateActions: false, ...irrelevantUIConfig }}
        />
        ,
      </MemoryRouter>,
    );
    expect(screen.queryByText("Cancel session")).toBeNull();
  });
});
