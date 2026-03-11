// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { render } from "@testing-library/react";
import * as H from "history";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { SWRConfig } from "swr";

import * as jobsApi from "src/api/jobsApi";

import { ClusterDetailsContext } from "../../contexts";
import { formatDuration } from "../util/duration";

import { JobsPage, JobsPageProps } from "./jobsPage";
import { allJobsFixture, earliestRetainedTime } from "./jobsPage.fixture";

import JobsResponse = cockroach.server.serverpb.JobsResponse;

jest.mock("src/api/jobsApi", () => {
  const actual = jest.requireActual("src/api/jobsApi");
  return {
    ...actual,
    useJobs: jest.fn(),
  };
});

const mockUseJobs = jobsApi.useJobs as jest.Mock;

const getMockJobsPageProps = (): JobsPageProps => {
  const history = H.createHashHistory();
  return {
    sort: { columnTitle: null, ascending: true },
    status: "",
    show: "50",
    type: 0,
    columns: [],
    setSort: () => {},
    setStatus: () => {},
    setShow: () => {},
    setType: () => {},
    onColumnsChange: () => {},
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

const renderWithProviders = (ui: React.ReactElement) => {
  return render(
    <SWRConfig value={{ provider: () => new Map() }}>
      <ClusterDetailsContext.Provider
        value={{ clusterId: "test", isTenant: false }}
      >
        <MemoryRouter>{ui}</MemoryRouter>
      </ClusterDetailsContext.Provider>
    </SWRConfig>,
  );
};

describe("Jobs", () => {
  beforeEach(() => {
    mockUseJobs.mockReturnValue({
      data: new JobsResponse({
        jobs: allJobsFixture,
        earliest_retained_time: earliestRetainedTime,
      }),
      error: null,
      isLoading: false,
      mutate: jest.fn(),
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
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
    const { getAllByText } = renderWithProviders(
      <JobsPage {...getMockJobsPageProps()} />,
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
    mockUseJobs.mockReturnValue({
      data: new JobsResponse({ jobs: [] }),
      error: null,
      isLoading: false,
      mutate: jest.fn(),
    });

    const { getByText } = renderWithProviders(
      <JobsPage {...getMockJobsPageProps()} />,
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
