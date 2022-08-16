// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";
import { propsToQueryString } from "../util";

const JOBS_PATH = "/_admin/v1/schedules";

export type SchedulesRequest = cockroach.server.serverpb.SchedulesRequest;
export type SchedulesResponse = cockroach.server.serverpb.SchedulesResponse;

export type ScheduleRequest = cockroach.server.serverpb.ScheduleRequest;
export type ScheduleResponse = cockroach.server.serverpb.ScheduleResponse;

export const getSchedules = (
  req: SchedulesRequest,
): Promise<cockroach.server.serverpb.SchedulesResponse> => {
  const queryStr = propsToQueryString({
    status: req.status,
    type: req.type.toString(),
    limit: req.limit,
  });
  return fetchData(
    cockroach.server.serverpb.SchedulesResponse,
    `${JOBS_PATH}?${queryStr}`,
    null,
    null,
    "30M",
  );
};

export const getSchedule = (
  req: ScheduleRequest,
): Promise<cockroach.server.serverpb.ScheduleResponse> => {
  return fetchData(
    cockroach.server.serverpb.ScheduleResponse,
    `${JOBS_PATH}/${req.schedule_id}`,
    null,
    null,
    "30M",
  );
};
