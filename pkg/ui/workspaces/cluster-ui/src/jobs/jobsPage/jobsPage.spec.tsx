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

import { formatDuration } from "../util/duration";

import { JobsPage, JobsPageProps } from "./jobsPage";
import { allJobsFixture, earliestRetainedTime } from "./jobsPage.fixture";

import Job = cockroach.server.serverpb.IJobResponse;

const getMockJobsPageProps = (jobs: Array<Job>): JobsPageProps => {
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
    jobsResponse: {
      data: {
        jobs,
        earliest_retained_time: earliestRetainedTime,
      },
      valid: true,
      lastUpdated: moment(),
      error: null,
      inFlight: false,
    },
    refreshJobs: () => {},
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

describe("Jobs", () => {
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
    const { getAllByText } = render(
      <MemoryRouter>
        <JobsPage {...getMockJobsPageProps(allJobsFixture)} />
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
    const { getByText } = render(
      <MemoryRouter>
        <JobsPage {...getMockJobsPageProps([])} />
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
