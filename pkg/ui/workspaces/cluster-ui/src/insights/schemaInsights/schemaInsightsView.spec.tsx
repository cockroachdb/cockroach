// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import * as schemaInsightsApi from "../../api/schemaInsightsApi";
import * as userApi from "../../api/userApi";

import { SchemaInsightsPropsFixture } from "./schemaInsights.fixture";
import { SchemaInsightsView } from "./schemaInsightsView";

describe("schemaInsightsView", () => {
  // let spy: jest.MockInstance<any, any>;
  // afterEach(() => {
  //   // spy.mockClear();
  // });

  it("renders expected table columns", async () => {
    const insightsSpy = jest
      .spyOn(schemaInsightsApi, "useSchemaInsights")
      .mockReturnValue({
        data: {
          maxSizeReached: false,
          results: SchemaInsightsPropsFixture,
        },
        isLoading: false,
        error: null,
        mutate: null,
        isValidating: false,
      });

    const userRoleApiSpy = jest
      .spyOn(userApi, "useUserSQLRoles")
      .mockReturnValue({
        data: { roles: ["ADMIN"] },
        isLoading: false,
        error: null,
        mutate: null,
        isValidating: false,
      });

    const { getAllByText } = render(
      <MemoryRouter>
        <SchemaInsightsView />
      </MemoryRouter>,
    );
    const dropIndexes = getAllByText("Drop Unused Index");
    expect(dropIndexes).toHaveLength(2);

    const createIndexes = getAllByText("Create Index");
    expect(createIndexes).toHaveLength(2);

    insightsSpy.mockClear();
    userRoleApiSpy.mockClear();
  });
});
