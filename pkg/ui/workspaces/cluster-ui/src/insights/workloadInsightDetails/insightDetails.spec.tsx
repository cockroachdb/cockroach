// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";
import { MemoryRouter as Router } from "react-router-dom";
import { createSandbox } from "sinon";

import * as sqlApi from "../../api/sqlApi";
import { SqlApiResponse } from "../../api/sqlApi";
import * as stmtInsightsApi from "../../api/stmtInsightsApi";
import { CollapseWhitespace, MockSqlResponse } from "../../util/testing";
import { StmtInsightEvent } from "../types";

import { getStatementInsightPropsFixture } from "./insightDetails.fixture";
import {
  StatementInsightDetails,
  StatementInsightDetailsProps,
} from "./statementInsightDetails";

const sandbox = createSandbox();

describe("StatementInsightDetails page", () => {
  let props: StatementInsightDetailsProps;

  beforeEach(() => {
    sandbox.reset();
    props = getStatementInsightPropsFixture();

    // The StmtInsights API will be triggered on render to refresh data.
    const resp: SqlApiResponse<StmtInsightEvent[]> = {
      maxSizeReached: false,
      results: [props.insightEventDetails],
    };
    jest
      .spyOn(stmtInsightsApi, "getStmtInsightsApi")
      .mockReturnValueOnce(Promise.resolve(resp));
  });

  it("shows loading indicator when data is not ready yet", async () => {
    // Clear insights to trigger a loading state.
    props.insightEventDetails = null;

    render(
      <Router>
        <StatementInsightDetails {...props} />
      </Router>,
    );

    screen.getByLabelText("Loading...");

    // Wait for the StmtInsights API call to refresh the data.
    await screen.findByText("Explain Plan");
  });

  it("shows two workload insights for a query", () => {
    render(
      <Router>
        <StatementInsightDetails {...props} />
      </Router>,
    );

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
    render(
      <Router>
        <StatementInsightDetails {...props} />
      </Router>,
    );

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
