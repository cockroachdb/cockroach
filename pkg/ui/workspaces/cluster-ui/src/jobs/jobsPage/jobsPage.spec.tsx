// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert, expect } from "chai";
import moment from "moment";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { JobsPage, JobsPageProps } from "./jobsPage";
import { formatDuration } from "../util/duration";
import {
  allJobsFixture,
  retryRunningJobFixture,
  earliestRetainedTime,
} from "./jobsPage.fixture";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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
    setSort: () => {},
    setStatus: () => {},
    setShow: () => {},
    setType: () => {},
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
    assert.equal(formatDuration(moment.duration(0)), "00:00:00");
    assert.equal(formatDuration(moment.duration(5, "minutes")), "00:05:00");
    assert.equal(formatDuration(moment.duration(5, "hours")), "05:00:00");
    assert.equal(formatDuration(moment.duration(110, "hours")), "110:00:00");
    assert.equal(
      formatDuration(moment.duration(12345, "hours")),
      "12345:00:00",
    );
  });

  it("renders expected jobs table columns", () => {
    const { getByText } = render(
      <MemoryRouter>
        <JobsPage {...getMockJobsPageProps(allJobsFixture)} />
      </MemoryRouter>,
    );
    const expectedColumnTitles = [
      "Description",
      "Status",
      "Job ID",
      "User",
      "Creation Time (UTC)",
      "Last Execution Time (UTC)",
      "Execution Count",
    ];

    for (const columnTitle of expectedColumnTitles) {
      getByText(columnTitle);
    }
  });

  it("renders a message when the table is empty", () => {
    const { getByText } = render(
      <MemoryRouter>
        <JobsPage {...getMockJobsPageProps([])} />
      </MemoryRouter>,
    );
    const expectedText = [
      "No jobs to show",
      "The jobs page provides details about backup/restore jobs, schema changes, user-created table statistics, automatic table statistics jobs and changefeeds.",
    ];

    for (const text of expectedText) {
      getByText(text);
    }
  });

  it("shows next execution time on hovering a retry status", async () => {
    const { getByText } = render(
      <MemoryRouter>
        <JobsPage {...getMockJobsPageProps([retryRunningJobFixture])} />
      </MemoryRouter>,
    );

    userEvent.hover(getByText("Retrying"));

    await waitFor(() =>
      screen.getByText("Next Execution Time:", { exact: false }),
    );
  });
});
