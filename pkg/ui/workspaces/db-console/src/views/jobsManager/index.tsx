// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import React from "react";
import { Helmet } from "react-helmet";

interface JobInfo {
  job_id: number;
  job_type: string;
  description: string;
  status: string;
  created: string;
}

interface JobsResponse {
  jobs: JobInfo[];
}

const JOBS_API_PATH = "api/v2/dbconsole/jobs-manager/jobs";

async function fetchJobs(): Promise<JobInfo[]> {
  const response = await fetch(JOBS_API_PATH, {
    headers: {
      Accept: "application/json",
      "X-Cockroach-API-Session": "cookie",
    },
    credentials: "same-origin",
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch jobs: ${response.status}`);
  }
  const data: JobsResponse = await response.json();
  return data.jobs ?? [];
}

async function controlJob(
  jobId: number,
  action: "pause" | "resume",
): Promise<void> {
  const response = await fetch(`${JOBS_API_PATH}/${jobId}`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Cockroach-API-Session": "cookie",
    },
    credentials: "same-origin",
    body: JSON.stringify({ action }),
  });
  if (!response.ok) {
    throw new Error(`Failed to ${action} job ${jobId}: ${response.status}`);
  }
}

function useJobs() {
  const { data, error, isLoading, mutate } =
    util.useSwrWithClusterId<JobInfo[]>("jobs-manager-jobs", fetchJobs, {
      revalidateOnFocus: false,
      refreshInterval: 5000,
    });

  const pauseJob = async (jobId: number) => {
    await controlJob(jobId, "pause");
    await mutate();
  };

  const resumeJob = async (jobId: number) => {
    await controlJob(jobId, "resume");
    await mutate();
  };

  return {
    jobs: data ?? [],
    error,
    isLoading,
    pauseJob,
    resumeJob,
    refresh: mutate,
  };
}

function JobsManagerPage(): React.ReactElement {
  const { jobs, isLoading, error, pauseJob, resumeJob } = useJobs();

  return (
    <div className="section">
      <Helmet title="Jobs Manager" />
      <section className="section">
        <h3 className="base-heading">Jobs Manager</h3>
        <p>
          View and control running and paused jobs. This page auto-refreshes
          every 5 seconds.
        </p>
      </section>
      <section className="section">
        {error ? (
          <p>Error loading jobs: {error.message}</p>
        ) : isLoading ? (
          <p>Loading jobs...</p>
        ) : jobs.length === 0 ? (
          <p>No running or paused jobs.</p>
        ) : (
          <table className="sort-table">
            <thead>
              <tr className="sort-table__row sort-table__row--header">
                <th className="sort-table__cell">Job ID</th>
                <th className="sort-table__cell">Type</th>
                <th className="sort-table__cell">Description</th>
                <th className="sort-table__cell">Status</th>
                <th className="sort-table__cell">Created</th>
                <th className="sort-table__cell">Action</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map(job => (
                <tr className="sort-table__row" key={job.job_id}>
                  <td className="sort-table__cell">{job.job_id}</td>
                  <td className="sort-table__cell">{job.job_type}</td>
                  <td className="sort-table__cell">{job.description}</td>
                  <td className="sort-table__cell">{job.status}</td>
                  <td className="sort-table__cell">{job.created}</td>
                  <td className="sort-table__cell">
                    {job.status === "running" ? (
                      <button onClick={() => pauseJob(job.job_id)}>
                        Pause
                      </button>
                    ) : (
                      <button onClick={() => resumeJob(job.job_id)}>
                        Resume
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

export default JobsManagerPage;
