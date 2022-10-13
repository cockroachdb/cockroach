// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { JobsPage, JobsPageProps } from "./jobsPage";
import { formatDuration } from "../util/duration";
import { allJobsFixture, earliestRetainedTime } from "./jobsPage.fixture";
import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import * as H from "history";

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
    jobs: {
      jobs: jobs,
      earliest_retained_time: earliestRetainedTime,
      toJSON: () => ({}),
    },
    jobsLoading: false,
    jobsError: null,
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
      "Last Execution Time (UTC)",
      "Execution Count",
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
