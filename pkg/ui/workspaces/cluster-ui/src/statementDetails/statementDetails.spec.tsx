// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";

import * as nodesApi from "../api/nodesApi";
import * as diagnosticsApi from "../api/statementDiagnosticsApi";
import * as statementsApi from "../api/statementsApi";
import * as insightsApi from "../api/stmtInsightsApi";
import * as userApi from "../api/userApi";
import { ClusterDetailsContext } from "../contexts";

import { StatementDetails, StatementDetailsProps } from "./statementDetails";
import {
  getStatementDetailsPropsFixture,
  statementDetailsFixtureData,
} from "./statementDetails.fixture";

jest.spyOn(diagnosticsApi, "useStatementDiagnostics").mockReturnValue({
  data: [
    {
      id: "123",
      statement_fingerprint: "SELECT * FROM crdb_internal.node_build_info",
      completed: true,
      requested_at: new Date("2021-12-08T09:51:27Z") as unknown,
      min_execution_latency: undefined,
      expires_at: undefined,
    },
  ],
  error: undefined,
  isLoading: false,
} as any);

jest
  .spyOn(diagnosticsApi, "useCreateDiagnosticsReport")
  .mockReturnValue({ createReport: jest.fn() });

jest
  .spyOn(diagnosticsApi, "useCancelDiagnosticsReport")
  .mockReturnValue({ cancelReport: jest.fn() });

jest.spyOn(nodesApi, "useNodes").mockReturnValue({
  nodeStatuses: [],
  nodeStatusByID: {},
  storeIDToNodeID: {},
  nodeRegionsByID: {
    "1": "gcp-us-east1",
    "2": "gcp-us-east1",
    "3": "gcp-us-west1",
    "4": "gcp-europe-west1",
  },
  isLoading: false,
  error: undefined,
} as any);

jest.spyOn(userApi, "useUserSQLRoles").mockReturnValue({
  data: { roles: ["ADMIN"] },
  error: undefined,
  isLoading: false,
  isValidating: false,
  mutate: jest.fn(),
} as any);

jest.spyOn(insightsApi, "useStmtFingerprintInsights").mockReturnValue({
  data: { results: [] },
  error: undefined,
  isLoading: false,
  isValidating: false,
  mutate: jest.fn(),
} as any);

const renderWithContext = (props: StatementDetailsProps) =>
  render(
    <ClusterDetailsContext.Provider value={{ isTenant: false }}>
      <Router>
        <StatementDetails {...props} />
      </Router>
    </ClusterDetailsContext.Provider>,
  );

describe("StatementDetails page", () => {
  let statementDetailsProps: StatementDetailsProps;

  beforeEach(() => {
    statementDetailsProps = getStatementDetailsPropsFixture();
  });

  it("shows loading indicator when data is not ready yet", () => {
    jest.spyOn(statementsApi, "useStatementDetails").mockReturnValue({
      data: null,
      error: undefined,
      isLoading: true,
      isValidating: false,
      mutate: jest.fn(),
    } as any);

    renderWithContext(statementDetailsProps);
    screen.getByLabelText("Loading...");
  });

  it("shows error alert when there is an error", () => {
    jest.spyOn(statementsApi, "useStatementDetails").mockReturnValue({
      data: undefined,
      error: new Error("Something went wrong"),
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    } as any);

    renderWithContext(statementDetailsProps);
    screen.getByText("Error message: Something went wrong;", { exact: false });
  });

  it("switches to the Diagnostics tab", () => {
    jest.spyOn(statementsApi, "useStatementDetails").mockReturnValue({
      data: statementDetailsFixtureData,
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    } as any);

    const onTabChangeSpy = jest.fn();

    renderWithContext({
      ...statementDetailsProps,
      onTabChanged: onTabChangeSpy,
    });

    fireEvent.click(screen.getByText("Diagnostics (1)"));
    expect(onTabChangeSpy).toHaveBeenCalledWith("diagnostics");
  });

  it("shows completed diagnostics report", () => {
    jest.spyOn(statementsApi, "useStatementDetails").mockReturnValue({
      data: statementDetailsFixtureData,
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    } as any);

    statementDetailsProps.history.location.search = new URLSearchParams([
      ["tab", "diagnostics"],
    ]).toString();

    renderWithContext(statementDetailsProps);
    screen.getByText("Dec 08, 2021 at 9:51 UTC", { exact: false });
  });
});
