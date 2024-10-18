// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RedoOutlined } from "@ant-design/icons";
import { Skeleton } from "antd";
import React, { useCallback, useEffect } from "react";

import {
  TableMetadataJobStatus,
  triggerUpdateTableMetaJobApi,
  useTableMetaUpdateJob,
} from "src/api/databases/tableMetaUpdateJobApi";
import { TABLE_METADATA_LAST_UPDATED_HELP } from "src/constants/tooltipMessages";
import Button from "src/sharedFromCloud/button";
import { Timestamp } from "src/timestamp";
import { DATE_WITH_SECONDS_FORMAT_24_TZ } from "src/util";
import { usePrevious } from "src/util/hooks";

import { Tooltip } from "../tooltip";

import styles from "./tableMetadataJobControl.module.scss";

type TableMetadataJobControlProps = {
  // Callback for when the job has updated the metadata, i.e. the
  // lastUpdatedTime has changed.
  onJobComplete?: () => void;
};

// This value is used to signal that the job completed
// time hasn't loaded yet.
const JOB_NOT_LOADED = -1;

export const TableMetadataJobControl: React.FC<
  TableMetadataJobControlProps
> = ({ onJobComplete }) => {
  const { jobStatus, refreshJobStatus, isLoading } = useTableMetaUpdateJob();
  const previousUpdateCompletedUnixSecs = usePrevious(
    isLoading ? JOB_NOT_LOADED : jobStatus?.lastCompletedTime?.unix(),
  );
  const lastUpdateCompletedUnixSecs = jobStatus?.lastCompletedTime?.unix();

  const triggerUpdateTableMetaJob = useCallback(
    async (onlyIfStale = true) => {
      const resp = await triggerUpdateTableMetaJobApi({ onlyIfStale });
      if (resp.jobTriggered) {
        return refreshJobStatus();
      }
    },
    [refreshJobStatus],
  );

  useEffect(() => {
    triggerUpdateTableMetaJob();
    const nextUpdated = setInterval(() => {
      triggerUpdateTableMetaJob();
    }, 10000);

    return () => clearTimeout(nextUpdated);
  }, [triggerUpdateTableMetaJob]);

  useEffect(() => {
    // If the last completed time has changed, call the callback.
    if (
      previousUpdateCompletedUnixSecs === JOB_NOT_LOADED ||
      previousUpdateCompletedUnixSecs === lastUpdateCompletedUnixSecs
    ) {
      return;
    }
    onJobComplete && onJobComplete();
  }, [
    previousUpdateCompletedUnixSecs,
    lastUpdateCompletedUnixSecs,
    onJobComplete,
  ]);

  const onRefreshClick = () => {
    // Force refresh.
    triggerUpdateTableMetaJob(false);
  };

  const durationText = jobStatus?.lastCompletedTime?.fromNow();
  const isRunning = jobStatus?.currentStatus === TableMetadataJobStatus.RUNNING;
  const refreshButtonTooltip = isRunning
    ? "Data is currently refreshing"
    : "Refresh data";

  return (
    <div className={styles["controls-container"]}>
      <Skeleton loading={isLoading}>
        <Tooltip title={TABLE_METADATA_LAST_UPDATED_HELP}>
          Last refreshed:{" "}
          <Timestamp
            format={DATE_WITH_SECONDS_FORMAT_24_TZ}
            time={jobStatus?.lastCompletedTime}
            fallback={"Never"}
          />{" "}
          {durationText && `(${durationText})`}
        </Tooltip>
      </Skeleton>
      <Tooltip placement="top" title={refreshButtonTooltip}>
        <div>
          <Button
            disabled={isRunning}
            category={"icon-container"}
            icon={<RedoOutlined />}
            onClick={onRefreshClick}
          />
        </div>
      </Tooltip>
    </div>
  );
};
