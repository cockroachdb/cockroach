// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import * as H from "history";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import * as utils from "src/util/hooks";

import { activeScheduleFixture } from "../schedulesPage/schedulesPage.fixture";

import { ScheduleDetails, ScheduleDetailsProps } from "./scheduleDetails";

const getMockScheduleDetailsProps = (
  scheduleID = "123",
): ScheduleDetailsProps => {
  const history = H.createHashHistory();
  return {
    location: history.location,
    history,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params: { id: scheduleID },
    },
  };
};

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
    const { getByText } = render(
      <MemoryRouter>
        <ScheduleDetails {...getMockScheduleDetailsProps()} />
      </MemoryRouter>,
    );
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

    const { getByText } = render(
      <MemoryRouter>
        <ScheduleDetails {...getMockScheduleDetailsProps()} />
      </MemoryRouter>,
    );
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

    const { getByTestId } = render(
      <MemoryRouter>
        <ScheduleDetails {...getMockScheduleDetailsProps()} />
      </MemoryRouter>,
    );
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

    const { getByText } = render(
      <MemoryRouter>
        <ScheduleDetails {...getMockScheduleDetailsProps("456")} />
      </MemoryRouter>,
    );
    getByText("Schedule ID: 456");
  });
});
