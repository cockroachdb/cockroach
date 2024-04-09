// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React from "react";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { render, screen } from "@testing-library/react";
import { makeTimestamp } from "src/util";
import {
  JOB_STATUS_RUNNING,
  ALL_JOB_STATUSES,
  JOB_STATUS_SUCCEEDED,
} from "./jobOptions";
import { Duration } from "./duration";

// Job running for 10 minutes
const START_SECONDS = 0;
const CURRENT_SECONDS = 600;

describe("<Duration>", () => {
  describe("when job is not running or completed", () => {
    it.each(
      Array.from(ALL_JOB_STATUSES).filter(
        s => s !== JOB_STATUS_RUNNING && s !== JOB_STATUS_SUCCEEDED,
      ),
    )("does not render anything when job status is %s", status => {
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
  });

  describe("when job is running", () => {
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

    it("only states that there is less than a minute remaining at less than 1 minute estimated", () => {
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
  });

  describe("when job has successfully completed", () => {
    it("states the duration of the job", () => {
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
});
