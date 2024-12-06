// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { useMemo } from "react";

import { useSwrWithClusterId } from "../../util";
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
// The hook polls the API every 3 seconds if the job status is parsed as and every
// 10 seconds otherwise.
export const useTableMetaUpdateJob = () => {
  const { data, isLoading, mutate } = useSwrWithClusterId(
    "tableMetaUpdateJob",
    getTableMetaUpdateJobInfo,
    {
      focusThrottleInterval: 10000,
      refreshInterval: (latest: TableMetaUpdateJobResponseServer) => {
        return latest?.current_status === TableMetadataJobStatus.RUNNING
          ? 3000
          : 10000;
      },
      dedupingInterval: 3000,
    },
  );

  const formattedResp: TableMetaUpdateJobInfo = useMemo(
    () => convertTableMetaUpdateJobFromServer(data),
    [data],
  );

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
