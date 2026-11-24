// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter, Route } from "react-router-dom";

import * as utils from "src/util/hooks";

import { activeScheduleFixture } from "../schedulesPage/schedulesPage.fixture";

import { ScheduleDetails } from "./scheduleDetails";

const renderWithRoute = (scheduleID = "123") =>
  render(
    <MemoryRouter initialEntries={[`/schedules/${scheduleID}`]}>
      <Route path="/schedules/:id">
        <ScheduleDetails />
      </Route>
    </MemoryRouter>,
  );

describe("ScheduleDetails", () => {
  let spy: jest.MockInstance<any, any>;
  afterEach(() => {
    spy.mockClear();
  });

  it("renders schedule details when data is loaded", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: activeScheduleFixture,
      isLoading: false,
      error: null,
      mutate: null,
      isValidating: false,
    });
    const { getByText } = renderWithRoute();
    getByText("Label");
    getByText("Status");
    getByText("State");
    getByText("Creation Time");
    getByText("Next Execution Time");
    getByText("Recurrence");
    getByText("Jobs Running");
    getByText("Owner");
  });

  it("renders an error message when there is an error retrieving data", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error("Error retrieving data"),
      mutate: null,
      isValidating: false,
    });

    const { getByText } = renderWithRoute();
    getByText("Error retrieving data");
  });

  it("renders a loading spinner when data is still loading", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
      mutate: null,
      isValidating: false,
    });

    const { getByTestId } = renderWithRoute();
    getByTestId("loading-spinner");
  });

  it("renders the schedule ID in the header", () => {
    spy = jest.spyOn(utils, "useSwrWithClusterId").mockReturnValue({
      data: activeScheduleFixture,
      isLoading: false,
      error: null,
      mutate: null,
      isValidating: false,
    });

    const { getByText } = renderWithRoute("456");
    getByText("Schedule ID: 456");
  });
});
