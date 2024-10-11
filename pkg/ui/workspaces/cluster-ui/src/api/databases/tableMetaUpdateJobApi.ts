// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { useEffect, useMemo } from "react";
import useSWR from "swr";

import { fetchDataJSON } from "../fetchData";

import {
  TableMetaUpdateJobResponseServer,
  TriggerTableMetaUpdateJobResponseServer,
} from "./serverTypes";

const TABLE_META_JOB_API = "api/v2/table_metadata/updatejob/";

export enum TableMetadataJobStatus {
  NOT_RUNNING = "NOT_RUNNING",
  RUNNING = "RUNNING",
}

export type TableMetaUpdateJobInfo = {
  currentStatus: TableMetadataJobStatus;
  progress: number;
  lastStartTime: moment.Moment | null;
  lastCompletedTime: moment.Moment | null;
  lastUpdatedTime: moment.Moment | null;
  dataValidDuration: moment.Duration;
  automaticUpdatesEnabled: boolean;
};

const getTableMetaUpdateJobInfo = async () => {
  return fetchDataJSON<TableMetaUpdateJobResponseServer, null>(
    TABLE_META_JOB_API,
  );
};

export const convertTableMetaUpdateJobFromServer = (
  data: TableMetaUpdateJobResponseServer,
): TableMetaUpdateJobInfo | null => {
  if (!data) {
    return null;
  }

  return {
    currentStatus: (data.current_status ??
      "NOT_RUNNING") as TableMetadataJobStatus,
    progress: data.progress ?? 0,
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
      refreshInterval: (latest: TableMetaUpdateJobResponseServer) => {
        return latest?.current_status === TableMetadataJobStatus.RUNNING
          ? 3000
          : 0;
      },
      dedupingInterval: 3000,
    },
  );

  const formattedResp: TableMetaUpdateJobInfo = useMemo(
    () => convertTableMetaUpdateJobFromServer(data),
    [data],
  );

  const dataValidDurationMs = formattedResp?.dataValidDuration.asMilliseconds();
  // Last completed is only non-null if the job has completed at least once.
  const lastCompletedTimeUnixMs =
    (formattedResp?.lastCompletedTime?.unix() ?? 0) * 1000;

  useEffect(() => {
    // This effect is triggered whenever the job's last completed time has changed.
    // It will schedule a job trigger request a little after when the data is scheduled
    // to expire: lastCompletedTime + dataValidDuration.
    // We add a 3 second slack to the delay.
    if (isLoading) {
      return;
    }

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

export type TriggerTableMetaUpdateJobResponse = {
  jobTriggered: boolean;
  message: string;
};

export const convertTriggerTableMetaUpdateJobResponseFromServer = (
  data: TriggerTableMetaUpdateJobResponseServer,
): TriggerTableMetaUpdateJobResponse => ({
  jobTriggered: data?.job_triggered ?? false,
  message: data?.message ?? "",
});

export const triggerUpdateTableMetaJobApi = async (
  req: TriggerTableMetaUpdateJobRequest,
): Promise<TriggerTableMetaUpdateJobResponse> => {
  const urlParams = new URLSearchParams();
  if (req.onlyIfStale) {
    urlParams.append("onlyIfStale", req.onlyIfStale.toString());
  }
  return fetchDataJSON<
    TriggerTableMetaUpdateJobResponseServer,
    TriggerTableMetaUpdateJobRequest
  >(TABLE_META_JOB_API + "?" + urlParams.toString(), req).then(
    convertTriggerTableMetaUpdateJobResponseFromServer,
  );
};
