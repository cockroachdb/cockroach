// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { render } from "@testing-library/react";

import { TransactionInsightsView } from "src/insights";
import { transactionInsightsPropsFixture } from "./transactionInsights.fixture";

describe("Transaction Insights", () => {
  test("renders expected workload insights table columns", () => {
    const { getByText } = render(
      <TransactionInsightsView {...transactionInsightsPropsFixture} />,
    );
    const expectedColumnTitles = [
      "Execution ID",
      "Execution",
      "Insights",
      "Start Time (UTC)",
      "Elapsed Time",
      "Application",
    ];

    for (const columnTitle of expectedColumnTitles) {
      getByText(columnTitle);
    }
  });

  test("renders a message when the table is empty", () => {
    const transactionInsightsPropsFixtureEmpty =
      transactionInsightsPropsFixture;
    transactionInsightsPropsFixtureEmpty.transactions = [];
    const { getByText } = render(
      <TransactionInsightsView {...transactionInsightsPropsFixtureEmpty} />,
    );
    const expectedText = [
      "No insight events since this page was last refreshed",
    ];

    for (const text of expectedText) {
      getByText(text);
    }
  });
});
