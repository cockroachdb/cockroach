// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import Long from "long";
import React from "react";
import { MemoryRouter, Route } from "react-router";
import { SWRConfig } from "swr";

import * as jobProfilerApi from "src/api/jobProfilerApi";
import * as jobApi from "src/api/jobsApi";
import * as userApi from "src/api/userApi";
import { CockroachCloudContext } from "src/contexts";

import { JOB_STATUS_RUNNING, JOB_STATUS_SUCCEEDED } from "../util";

import { JobDetailsV2 } from "./jobDetails";

const mockGetJob = (jobStatus: string | null) => {
  jest.spyOn(jobApi, "getJob").mockResolvedValue({
    id: Long.fromNumber(1),
    status: jobStatus,
    running_status: "test running status",
    type: "test job type",
    statement: "test job statement",
    description: "test job description",
    username: "test user",
    descriptor_ids: [],
    fraction_completed: jobStatus === JOB_STATUS_SUCCEEDED ? 1 : 0.5,
    error: "",
    highwater_decimal: "1",
    execution_failures: [],
    coordinator_id: Long.fromNumber(1),
    messages: [],
    num_runs: Long.fromNumber(1),
  });
};

const mockFetchExecutionDetailFiles = jest
  .spyOn(jobProfilerApi, "listExecutionDetailFiles")
  .mockResolvedValue({
    files: ["file1", "file2"],
  } as any);

const _mockCollectExecutionDetails = jest
  .spyOn(jobProfilerApi, "collectExecutionDetails")
  .mockImplementation(() => {
    mockFetchExecutionDetailFiles.mockResolvedValue({
      files: ["file1", "file2", "file3"],
    } as any);
    return Promise.resolve({ req_resp: true } as any);
  });

jest
  .spyOn(jobProfilerApi, "getExecutionDetailFile")
  .mockResolvedValue({} as any);

describe("JobDetailsV2", () => {
  beforeEach(() => {
    jest.spyOn(userApi, "useUserSQLRoles").mockReturnValue({
      data: { roles: ["ADMIN"] },
      isLoading: false,
      error: null,
      mutate: jest.fn(),
      isValidating: false,
    } as any);
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
  });

  const renderPage = () => {
    return render(
      <SWRConfig value={{ provider: () => new Map() }}>
        <CockroachCloudContext.Provider value={false}>
          <MemoryRouter initialEntries={["/job/1"]}>
            <Route path="/job/:id">
              <JobDetailsV2 />
            </Route>
          </MemoryRouter>
        </CockroachCloudContext.Provider>
      </SWRConfig>,
    );
  };

  it("refreshes job details periodically", async () => {
    jest.useFakeTimers();
    mockGetJob(JOB_STATUS_RUNNING);
    const { getByText } = renderPage();
    await waitFor(() => getByText("50.0%"));
    mockGetJob(JOB_STATUS_SUCCEEDED);
    jest.advanceTimersByTime(10 * 1000);
    await waitFor(() => getByText(JOB_STATUS_SUCCEEDED));
  });

  it("refetches execution files after collection", async () => {
    mockGetJob(JOB_STATUS_SUCCEEDED);
    const { getByText, queryByText } = renderPage();
    await waitFor(() => getByText(JOB_STATUS_SUCCEEDED));
    userEvent.click(getByText("Advanced Debugging"));
    await waitFor(() => getByText("file1"));
    getByText("file2");
    expect(queryByText("file3")).toBeNull();
    userEvent.click(getByText("Request Execution Details"));
    await waitFor(() => getByText("file3"));
  });
});
