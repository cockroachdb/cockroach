// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { LoadingOutlined, RedoOutlined } from "@ant-design/icons";
import { Spin } from "antd";
import React, { useCallback, useEffect } from "react";

import {
  TableMetadataJobStatus,
  triggerUpdateTableMetaJobApi,
  useTableMetaUpdateJob,
} from "src/api/databases/tableMetaUpdateJobApi";
import Button from "src/sharedFromCloud/button";
import { usePrevious } from "src/util/hooks";

import { Tooltip } from "../tooltip";
import { TableMetadataLastUpdatedTooltip } from "../tooltipMessages";

import styles from "./tableMetadataJobControl.module.scss";
import { TableMetadataJobProgress } from "./tableMetadataJobProgress";

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
      try {
        const resp = await triggerUpdateTableMetaJobApi({ onlyIfStale });
        if (resp.jobTriggered) {
          return refreshJobStatus();
        }
      } catch {
        // We don't need to do anything with additional errors right now.
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

  const isRunning = jobStatus?.currentStatus === TableMetadataJobStatus.RUNNING;
  const refreshButtonTooltip = isRunning ? (
    <TableMetadataJobProgress
      jobStartedTime={jobStatus?.lastStartTime}
      jobProgressFraction={jobStatus?.progress}
    />
  ) : (
    "Refresh data"
  );

  return (
    <div className={styles["controls-container"]}>
      <TableMetadataLastUpdatedTooltip
        loading={isLoading}
        timestamp={jobStatus?.lastCompletedTime}
      >
        {durationText => <>Last refreshed: {durationText}</>}
      </TableMetadataLastUpdatedTooltip>
      <Tooltip noUnderline placement="top" title={refreshButtonTooltip}>
        <>
          <Button
            disabled={isRunning}
            category={"icon-container"}
            icon={
              isRunning ? (
                <Spin indicator={<LoadingOutlined spin />} size={"small"} />
              ) : (
                <RedoOutlined />
              )
            }
            onClick={onRefreshClick}
          />
        </>
      </Tooltip>
    </div>
  );
};
