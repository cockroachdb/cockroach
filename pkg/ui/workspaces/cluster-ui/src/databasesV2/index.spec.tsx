// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter, Route, Switch } from "react-router-dom";

import * as dbApi from "src/api/databases/getDatabaseMetadataApi";
import { CockroachCloudContext } from "src/contexts";

import { DatabasesPageV2 } from "./";

jest.mock("src/api/fetchData", () => ({
  fetchDataJSON: jest.fn().mockResolvedValue(null),
  fetchData: jest.fn().mockResolvedValue(null),
}));

const mockDatabaseMetadata = (errorStatus: number | null, data?: null | []) => {
  jest.spyOn(dbApi, "useDatabaseMetadata").mockReturnValue({
    error: errorStatus ? { status: errorStatus } : null,
    refreshDatabases: jest.fn(),
    data: data
      ? {
          results: data,
          pagination: {
            pageSize: 1,
            pageNum: 1,
            totalResults: 0,
          },
        }
      : null,
    isLoading: false,
  });
};

describe("DatabasesPageV2 redirect", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  const renderWithRouter = (initialEntries: string[], isCloud = false) =>
    render(
      <CockroachCloudContext.Provider value={isCloud}>
        <MemoryRouter initialEntries={initialEntries}>
          <Switch>
            <Route path="/databases">
              <DatabasesPageV2 />
            </Route>
            <Route path="/legacy/databases">
              <div>Legacy Databases Page</div>
            </Route>
          </Switch>
        </MemoryRouter>
      </CockroachCloudContext.Provider>,
    );

  it("redirects to /legacy/databases on 409 error", () => {
    mockDatabaseMetadata(409);

    renderWithRouter(["/databases"]);

    expect(screen.getByText("Legacy Databases Page")).toBeInTheDocument();
  });

  it.each([
    { error: 400, data: null },
    { error: 500, data: null },
    { error: null, data: [] },
  ])("does not redirect on other responses", ({ error, data }) => {
    mockDatabaseMetadata(error, data);

    renderWithRouter(["/databases"]);

    jest.clearAllMocks();
    expect(screen.queryByText("Legacy Databases Page")).not.toBeInTheDocument();
  });

  it("does not redirect on 409 if on cloud", () => {
    mockDatabaseMetadata(409);

    renderWithRouter(["/databases"], true);

    expect(screen.queryByText("Legacy Databases Page")).not.toBeInTheDocument();
  });
});
