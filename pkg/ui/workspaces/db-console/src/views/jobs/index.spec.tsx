// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import moment from "moment";
import { cockroach } from "src/js/protos";
import { formatDuration } from ".";
import { JobsTable, JobsTableProps } from "src/views/jobs/index";
import {
  allJobsFixture,
  retryRunningJobFixture,
} from "src/views/jobs/jobsTable.fixture";
import { refreshJobs, refreshSettings } from "src/redux/apiReducers";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import * as H from "history";

import { expectPopperTooltipActivated } from "src/test-utils/tooltip";

import Job = cockroach.server.serverpb.IJobResponse;

const getMockJobsTableProps = (jobs: Array<Job>): JobsTableProps => {
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
      data: {
        jobs: jobs,
        toJSON: () => ({}),
      },
      inFlight: false,
      valid: true,
    },
    retentionTime: moment.duration(336, "hours"),
    refreshJobs,
    refreshSettings,
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
        <JobsTable {...getMockJobsTableProps(allJobsFixture)} />
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

  it("shows next execution time on hovering a retry status", async () => {
    const { getByText } = render(
      <MemoryRouter>
        <JobsTable {...getMockJobsTableProps([retryRunningJobFixture])} />
      </MemoryRouter>,
    );

    await waitFor(expectPopperTooltipActivated);
    userEvent.hover(getByText("retrying"));

    await waitFor(() =>
      screen.getByText("Next Execution Time", { exact: false }),
    );
  });
});
