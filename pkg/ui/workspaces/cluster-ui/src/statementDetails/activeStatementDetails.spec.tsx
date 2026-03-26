// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";
import { MemoryRouter, Route } from "react-router-dom";

import * as liveWorkloadApi from "../api/liveWorkloadApi";
import * as sqlApi from "../api/sqlApi";
import * as userApi from "../api/userApi";
import { MockSqlResponse } from "../util/testing";

import { ActiveStatementDetails } from "./activeStatementDetails";
import { getLiveWorkloadFixture } from "./activeStatementDetails.fixture";

const STMT_EXECUTION_ID = "17ab864032f8e1c20000000000000001";

const renderPage = () =>
  render(
    <MemoryRouter
      initialEntries={[`/execution/statement/${STMT_EXECUTION_ID}`]}
    >
      <Route path="/execution/statement/:execution_id">
        <ActiveStatementDetails />
      </Route>
    </MemoryRouter>,
  );

describe("ActiveStatementDetails page", () => {
  beforeEach(() => {
    jest.spyOn(userApi, "useUserSQLRoles").mockReturnValue({
      data: { roles: ["ADMIN"] },
      isLoading: false,
      error: null,
      mutate: jest.fn(),
      isValidating: false,
    } as any);
    jest
      .spyOn(liveWorkloadApi, "useLiveWorkload")
      .mockReturnValue(getLiveWorkloadFixture());
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("uses immutable SWR to preserve cached data", () => {
    renderPage();

    expect(liveWorkloadApi.useLiveWorkload).toHaveBeenCalledWith({
      immutable: true,
    });
  });

  it("shows 'not found' when statement is missing from cache", () => {
    const fixture = getLiveWorkloadFixture();
    fixture.data.statements = [];
    jest.restoreAllMocks();
    jest.spyOn(userApi, "useUserSQLRoles").mockReturnValue({
      data: { roles: ["ADMIN"] },
      isLoading: false,
      error: null,
      mutate: jest.fn(),
      isValidating: false,
    } as any);
    jest.spyOn(liveWorkloadApi, "useLiveWorkload").mockReturnValue(fixture);

    renderPage();

    screen.getAllByText((_, e) => e.textContent === "SQL Execution not found.");
  });

  it("shows information about the active statement", () => {
    renderPage();

    screen.getAllByText(
      (_, e) => e.textContent === "SELECT count(*) FROM foo",
      { exact: false },
    );
    screen.getByText("Dec 12, 2021 at 0:00 UTC", { exact: false });
    screen.getByText("Executing", { exact: false });
    screen.getByText("my-app", { exact: false });
    screen.getByText("andy", { exact: false });
    screen.getByText("127.0.0.1", { exact: false });
    screen.getByText("123456789", { exact: false });
    screen.getByText("fac8885a-f40a-4666-b746-a45061faad74", {
      exact: false,
    });
  });

  it("switches to the Explain Plan tab and shows the plan", async () => {
    renderPage();

    // Click on the Explain tab. Mock executeInternalSql, which should be called
    // in order to decode the plan gist and .
    const planResponse = MockSqlResponse([
      {
        plan_row:
          "• group (scalar)\n" +
          "│\n" +
          "└── • scan\n" +
          "      table: foo@foo_pkey\n" +
          "      spans: FULL SCAN",
      },
    ]);
    const indexResponse = MockSqlResponse([
      {
        index_recommendations: [
          "creation : CREATE INDEX ON defaultdb.public.foo (y);",
        ],
      },
    ]);
    const explainPlanSpy = jest
      .spyOn(sqlApi, "executeInternalSql")
      .mockReturnValueOnce(Promise.resolve(planResponse))
      .mockReturnValueOnce(Promise.resolve(indexResponse));

    fireEvent.click(screen.getByText("Explain Plan"));
    expect(explainPlanSpy).toHaveBeenCalled();

    await screen.findByText("Plan Gist: AgICABoCBQQf0AEB", { exact: false });
    await screen.findByText("foo@foo_pkey", { exact: false });
    await screen.findByText("CREATE INDEX ON defaultdb.public.foo (y);", {
      exact: false,
    });
  });
});
