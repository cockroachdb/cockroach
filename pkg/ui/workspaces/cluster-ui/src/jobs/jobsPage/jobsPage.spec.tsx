// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter, Route } from "react-router-dom";
import useSWR from "swr";

import { formatDuration } from "../util/duration";

import { JobsPage } from "./jobsPage";
import { allJobsFixture, earliestRetainedTime } from "./jobsPage.fixture";

// Mock SWR to control the data returned to the component
jest.mock("swr");
const mockedSWR = useSWR as jest.Mock;

describe("Jobs", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("format duration", () => {
    expect(formatDuration(moment.duration(0))).toEqual("00:00:00");
    expect(formatDuration(moment.duration(5, "minutes"))).toEqual("00:05:00");
    expect(formatDuration(moment.duration(5, "hours"))).toEqual("05:00:00");
    expect(formatDuration(moment.duration(110, "hours"))).toEqual("110:00:00");
    expect(formatDuration(moment.duration(12345, "hours"))).toEqual(
      "12345:00:00",
    );
  });

  it("renders expected jobs table columns", () => {
    mockedSWR.mockReturnValue({
      data: {
        jobs: allJobsFixture,
        earliest_retained_time: earliestRetainedTime,
      },
      error: null,
      isValidating: false,
    });
    const { getAllByText } = render(
      <MemoryRouter initialEntries={["/jobs"]}>
        <Route path="/jobs" component={JobsPage} />
      </MemoryRouter>,
    );
    const expectedColumnTitles = [
      "Description",
      "Status",
      "Job ID",
      "User Name",
      "Creation Time (UTC)",
      "Last Modified Time (UTC)",
    ];

    for (const columnTitle of expectedColumnTitles) {
      getAllByText(columnTitle);
    }
  });

  it("renders a message when the table is empty", () => {
    mockedSWR.mockReturnValue({
      data: {
        jobs: [],
        earliest_retained_time: earliestRetainedTime,
      },
      error: null,
      isValidating: false,
    });
    const { getByText } = render(
      <MemoryRouter initialEntries={["/jobs"]}>
        <Route path="/jobs" component={JobsPage} />
      </MemoryRouter>,
    );
    const expectedText = [
      "No jobs found.",
      "The jobs page provides details about backup/restore jobs, schema changes, user-created table statistics, automatic table statistics jobs and changefeeds.",
    ];

    for (const text of expectedText) {
      getByText(text);
    }
  });
});
