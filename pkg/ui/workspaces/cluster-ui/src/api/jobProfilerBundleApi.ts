
// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
    executeInternalSql,
    LARGE_RESULT_SIZE,
    SqlExecutionRequest,
    sqlResultsAreEmpty,
} from "src/api";

export type JobProfilerBundle = {
    job_id: string;
    info_key: string;
    written: moment.Moment;
};

export type JobProfilerBundleResponse = JobProfilerBundle[];

export function getJobProfilerBundles(): Promise<JobProfilerBundleResponse> {
    const req: SqlExecutionRequest = {
        statements: [
            {
                sql: `SELECT
      job_id::STRING,
      info_key,
      written
    FROM
      system.job_info
    WHERE
      info_key LIKE '~profiler-bundle-metadata-%'`,
            },
        ],
        execute: true,
        max_result_size: LARGE_RESULT_SIZE,
    };

    return executeInternalSql<JobProfilerBundle>(req).then(res => {
        // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
        if (res.error) {
            throw res.error;
        }

        if (sqlResultsAreEmpty(res)) {
            return [];
        }

        return res.execution.txn_results[0].rows;
    });
}

export type InsertJobProfilerBundleRequest = {
    jobID: string;
};

export type InsertJobProfilerBundleResponse = {
    req_resp: boolean;
};

export function createJobProfilerBundle({
    jobID,
}: InsertJobProfilerBundleRequest): Promise<InsertJobProfilerBundleResponse> {
    const args: any = [jobID];

    const createBundle = {
        sql: `SELECT crdb_internal.request_job_profiler_bundle($1) as req_resp`,
        arguments: args,
    };

    const req: SqlExecutionRequest = {
        execute: true,
        statements: [createBundle],
    };

    return executeInternalSql<InsertJobProfilerBundleResponse>(req).then(res => {
        // If request succeeded but query failed, throw error (caught by saga/cacheDataReducer).
        if (res.error) {
            console.log("throwing error")
            console.log(res.error)
            throw res.error;
        }

        if (
            res.execution?.txn_results[0]?.rows?.length === 0 ||
            res.execution?.txn_results[0]?.rows[0]["req_resp"] === false
        ) {
            throw new Error("Failed to insert job profiler bundle request");
        }

        return res.execution.txn_results[0].rows[0];
    });
}