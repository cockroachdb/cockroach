// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";
import { SWRConfig } from "swr";

import * as sqlApi from "../../api/sqlApi";
import * as stmtInsightsApi from "../../api/stmtInsightsApi";
import { ClusterDetailsContext } from "../../contexts";
import { CollapseWhitespace, MockSqlResponse } from "../../util/testing";

import {
  getStatementInsightPropsFixture,
  insightEventFixture,
} from "./insightDetails.fixture";
import {
  StatementInsightDetails,
  StatementInsightDetailsProps,
} from "./statementInsightDetails";

// Mock the SWR hook used by StatementInsightDetails.
jest.mock("../../api/stmtInsightsApi", () => {
  const actual = jest.requireActual("../../api/stmtInsightsApi");
  return {
    ...actual,
    useStmtInsightDetails: jest.fn(),
  };
});

const useStmtInsightDetailsMock =
  stmtInsightsApi.useStmtInsightDetails as jest.Mock;

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <SWRConfig value={{ provider: () => new Map() }}>
      <ClusterDetailsContext.Provider
        value={{
          isTenant: false,
          clusterId: "test-cluster",
        }}
      >
        <Router>{ui}</Router>
      </ClusterDetailsContext.Provider>
    </SWRConfig>,
  );
}

describe("StatementInsightDetails page", () => {
  let props: StatementInsightDetailsProps;

  beforeEach(() => {
    props = getStatementInsightPropsFixture();
  });

  it("shows loading indicator when data is not ready yet", async () => {
    // SWR hook returns no data yet (loading state).
    useStmtInsightDetailsMock.mockReturnValue({
      data: undefined,
      error: undefined,
      isLoading: true,
      isValidating: true,
      mutate: jest.fn(),
    });

    renderWithProviders(<StatementInsightDetails {...props} />);

    screen.getByLabelText("Loading...");

    // Now simulate data arriving.
    useStmtInsightDetailsMock.mockReturnValue({
      data: {
        maxSizeReached: false,
        results: [insightEventFixture],
      },
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    });

    renderWithProviders(<StatementInsightDetails {...props} />);

    await screen.findByText("Explain Plan");
  });

  it("shows two workload insights for a query", () => {
    useStmtInsightDetailsMock.mockReturnValue({
      data: {
        maxSizeReached: false,
        results: [insightEventFixture],
      },
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    });

    renderWithProviders(<StatementInsightDetails {...props} />);

    // Query should be shown in UI.
    screen.getAllByText(
      (_, e) =>
        CollapseWhitespace(e.textContent) === "SELECT count(*) FROM foo",
    );

    // Two insights should be shown.
    screen.getByText("Slow Execution");
    screen.getByText("Suboptimal Plan");
  });

  it("switches to the Explain Plan tab and shows the plan", async () => {
    useStmtInsightDetailsMock.mockReturnValue({
      data: {
        maxSizeReached: false,
        results: [insightEventFixture],
      },
      error: undefined,
      isLoading: false,
      isValidating: false,
      mutate: jest.fn(),
    });

    renderWithProviders(<StatementInsightDetails {...props} />);

    // Click on the Explain tab. Mock a response to executeInternalSql, which
    // should be called in order to decode the plan gist.
    const resp = MockSqlResponse([{ plan_row: "SHOW DATABASE" }]);
    const explainPlanSpy = jest
      .spyOn(sqlApi, "executeInternalSql")
      .mockReturnValueOnce(Promise.resolve(resp));

    fireEvent.click(screen.getByText("Explain Plan"));
    expect(explainPlanSpy).toHaveBeenCalled();

    await screen.findByText("Plan Gist: AgGA////nxkAAAYAAAADBQIGAg==", {
      exact: false,
    });
    screen.getByText("SHOW");
  });
});
