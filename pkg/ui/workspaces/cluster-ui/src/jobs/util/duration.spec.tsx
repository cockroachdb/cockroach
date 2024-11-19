// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { render, screen } from "@testing-library/react";
import React from "react";

import { makeTimestamp } from "src/util";

import { Duration } from "./duration";
import {
  JOB_STATUS_RUNNING,
  JOB_STATUS_SUCCEEDED,
  JOB_STATUS_FAILED,
  JOB_STATUS_CANCELED,
  JOB_STATUS_CANCEL_REQUESTED,
  JOB_STATUS_PAUSED,
  JOB_STATUS_PAUSE_REQUESTED,
  JOB_STATUS_PENDING,
  JOB_STATUS_REVERTING,
  JOB_STATUS_REVERT_FAILED,
} from "./jobOptions";

// Job running for 10 minutes
const START_SECONDS = 0;
const CURRENT_SECONDS = 600;

describe("<Duration>", () => {
  it.each([
    JOB_STATUS_FAILED,
    JOB_STATUS_CANCELED,
    JOB_STATUS_CANCEL_REQUESTED,
    JOB_STATUS_PAUSED,
    JOB_STATUS_PAUSE_REQUESTED,
    JOB_STATUS_PENDING,
    JOB_STATUS_REVERTING,
    JOB_STATUS_REVERT_FAILED,
  ])("does not render anything when job status is %s", status => {
    const job: cockroach.server.serverpb.IJobResponse = {
      started: null,
      modified: null,
      finished: null,
      status: status,
      fraction_completed: 0,
    };
    const { container } = render(<Duration job={job} />);
    expect(container.firstChild).toBeNull();
  });

  it("states that job is initializing at less than 5% completed", () => {
    const job: cockroach.server.serverpb.IJobResponse = {
      started: makeTimestamp(START_SECONDS),
      modified: makeTimestamp(CURRENT_SECONDS),
      finished: null,
      status: JOB_STATUS_RUNNING,
      fraction_completed: 0.02,
    };
    render(<Duration job={job} />);
    expect(screen.getByText("Initializing..."));
  });

  it("states the remaining time at more than 5% completed and more than 1 minute estimated", () => {
    const job: cockroach.server.serverpb.IJobResponse = {
      started: makeTimestamp(START_SECONDS),
      modified: makeTimestamp(CURRENT_SECONDS),
      finished: null,
      status: JOB_STATUS_RUNNING,
      fraction_completed: 0.24,
    };
    render(<Duration job={job} />);
    expect(screen.getByText(/\d\d:\d\d:\d\d remaining/));
  });

  it("states that there is less than a minute remaining at less than 1 minute estimated", () => {
    const job: cockroach.server.serverpb.IJobResponse = {
      started: makeTimestamp(START_SECONDS),
      modified: makeTimestamp(CURRENT_SECONDS),
      finished: null,
      status: JOB_STATUS_RUNNING,
      fraction_completed: 0.99,
    };
    render(<Duration job={job} />);
    expect(screen.getByText("Less than a minute remaining"));
  });

  it("states the duration of the job for completed jobs", () => {
    const job: cockroach.server.serverpb.IJobResponse = {
      started: makeTimestamp(START_SECONDS),
      modified: makeTimestamp(CURRENT_SECONDS),
      finished: makeTimestamp(CURRENT_SECONDS),
      status: JOB_STATUS_SUCCEEDED,
      fraction_completed: 1,
    };
    render(<Duration job={job} />);
    expect(screen.getByText(/Duration: \d\d:\d\d:\d\d/));
  });
});
