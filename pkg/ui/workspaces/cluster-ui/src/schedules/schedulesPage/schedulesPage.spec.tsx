// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import * as H from "history";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import * as utils from "src/util/hooks";

import { SchedulesPage, SchedulesPageProps } from "./schedulesPage";
import { allSchedulesFixture } from "./schedulesPage.fixture";

const getMockSchedulesPageProps = (): SchedulesPageProps => {
  const history = H.createHashHistory();
  return {
    sort: { columnTitle: null, ascending: true },
    setSort: () => {},
    location: history.location,
    history,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params: {},
    },
  };
};

describe("Schedules", () => {
  let spy: jest.MockInstance<any, any>;
  afterEach(() => {
    spy.mockClear();
  });

  it("renders expected schedules table columns", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: allSchedulesFixture,
      isLoading: false,
      error: null,
      mutate: null,
      isValidating: false,
    });
    const { getByText } = render(
      <MemoryRouter>
        <SchedulesPage {...getMockSchedulesPageProps()} />
      </MemoryRouter>,
    );
    const expectedColumnTitles = [
      "Label",
      "Status",
      "Schedule ID",
      "Owner",
      "Recurrence",
      "Creation Time (UTC)",
      "Next Execution Time (UTC)",
      "Jobs Running",
    ];

    for (const columnTitle of expectedColumnTitles) {
      getByText(columnTitle);
    }
    spy.mockClear();
  });

  it("renders a message when the table is empty", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
      mutate: null,
      isValidating: false,
    });

    const { getByText } = render(
      <MemoryRouter>
        <SchedulesPage {...getMockSchedulesPageProps()} />
      </MemoryRouter>,
    );
    getByText("No schedules to show");
  });

  it("renders an error message when there is an error retrieving data", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: [],
      isLoading: false,
      error: new Error("Error retrieving data"),
      mutate: null,
      isValidating: false,
    });

    const { getByText } = render(
      <MemoryRouter>
        <SchedulesPage {...getMockSchedulesPageProps()} />
      </MemoryRouter>,
    );
    getByText("Error retrieving data");
  });

  it("renders a loading spinner when data is still loading", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: [],
      isLoading: true,
      error: null,
      mutate: null,
      isValidating: false,
    });

    const { getByTestId } = render(
      <MemoryRouter>
        <SchedulesPage {...getMockSchedulesPageProps()} />
      </MemoryRouter>,
    );
    getByTestId("loading-spinner");
  });
});
