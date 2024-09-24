// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";
import { useEffect, useMemo } from "react";
import useSWR from "swr";

import { fetchDataJSON } from "../fetchData";

const TABLE_META_JOB_API = "api/v2/table_metadata/updatejob/";

export enum TableMetadataJobStatus {
  NOT_RUNNING = "NOT_RUNNING",
  RUNNING = "RUNNING",
}

type TableMetaUpdateJobResponse = {
  current_status: TableMetadataJobStatus;
  progress: number;
  last_start_time?: string;
  last_completed_time: string;
  last_updated_time: string;
  data_valid_duration: number;
  automatic_updates_enabled: boolean;
};

type TableMetaUpdateJobInfo = {
  currentStatus: TableMetaUpdateJobResponse["current_status"];
  progress: number;
  lastStartTime: moment.Moment;
  lastCompletedTime: moment.Moment;
  lastUpdatedTime: moment.Moment;
  dataValidDuration: moment.Duration;
  automaticUpdatesEnabled: boolean;
};

const getTableMetaUpdateJobInfo = async () => {
  return fetchDataJSON<TableMetaUpdateJobResponse, null>(TABLE_META_JOB_API);
};

// useTableMetaUpdateJob is a hook that fetches the current status of the table
// metadata update job and returns the status and other relevant information.
// The hook polls the API every 3 seconds if the job status is parsed as running.
export const useTableMetaUpdateJob = () => {
  const { data, isLoading, mutate } = useSWR(
    "tableMetaUpdateJob",
    getTableMetaUpdateJobInfo,
    {
      focusThrottleInterval: 10000,
      refreshInterval: (latest: TableMetaUpdateJobResponse) => {
        return latest?.current_status === TableMetadataJobStatus.RUNNING
          ? 3000
          : 0;
      },
      dedupingInterval: 3000,
    },
  );

  const formattedResp: TableMetaUpdateJobInfo = useMemo(() => {
    if (!data) {
      return null;
    }
    return {
      currentStatus: data.current_status,
      progress: data.progress,
      lastStartTime: data.last_start_time
        ? moment.utc(data.last_start_time)
        : null,
      lastCompletedTime: data.last_completed_time
        ? moment.utc(data.last_completed_time)
        : null,
      lastUpdatedTime: data.last_updated_time
        ? moment.utc(data.last_updated_time)
        : null,
      dataValidDuration: moment.duration(
        data.data_valid_duration * 1e-6,
        "millisecond",
      ),
      automaticUpdatesEnabled: data.automatic_updates_enabled,
    };
  }, [data]);

  const dataValidDurationMs =
    formattedResp?.dataValidDuration?.asMilliseconds();
  const lastCompletedTimeUnixMs =
    formattedResp?.lastCompletedTime?.unix() * 1000;
  useEffect(() => {
    if (isLoading) {
      return;
    }

    // Trigger a refresh a little after when the data is scheduled to
    // expire. We add a 3 second slack in case an update is triggered
    // around then.
    const msSinceLastCompletedWithSlack =
      Date.now() - lastCompletedTimeUnixMs + 3000;
    const delay = Math.max(
      0,
      dataValidDurationMs - msSinceLastCompletedWithSlack,
    );
    const nextUpdated = setTimeout(() => {
      return mutate();
    }, delay);

    return () => clearTimeout(nextUpdated);
  }, [dataValidDurationMs, isLoading, lastCompletedTimeUnixMs, mutate]);

  return {
    jobStatus: formattedResp,
    refreshJobStatus: mutate,
    isLoading,
  };
};

type TriggerTableMetaUpdateJobRequest = {
  onlyIfStale?: boolean;
};
type TriggerTableMetaUpdateJobResponse = {
  job_triggered: boolean;
  message: string;
};

export const triggerUpdateTableMetaJobApi = async (
  req: TriggerTableMetaUpdateJobRequest,
) => {
  const urlParams = new URLSearchParams();
  if (req.onlyIfStale) {
    urlParams.append("onlyIfStale", req.onlyIfStale.toString());
  }
  return fetchDataJSON<
    TriggerTableMetaUpdateJobResponse,
    TriggerTableMetaUpdateJobRequest
  >(TABLE_META_JOB_API + "?" + urlParams.toString(), req);
};
