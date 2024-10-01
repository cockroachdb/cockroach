// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RedoOutlined } from "@ant-design/icons";
import { Skeleton, Tooltip } from "antd";
import React, { useCallback, useEffect } from "react";

import {
  TableMetadataJobStatus,
  triggerUpdateTableMetaJobApi,
  useTableMetaUpdateJob,
} from "src/api/databases/tableMetaUpdateJobApi";
import Button from "src/sharedFromCloud/button";
import { usePrevious } from "src/util/hooks";

import styles from "./tableMetadataJobControl.module.scss";

type TableMetadataJobControlProps = {
  // Callback for when the job has updated the metadata, i.e. the
  // lastUpdatedTime has changed.
  onDataUpdated?: () => void;
};

export const TableMetadataJobControl: React.FC<
  TableMetadataJobControlProps
> = ({ onDataUpdated }) => {
  const { jobStatus, refreshJobStatus, isLoading } = useTableMetaUpdateJob();
  const previousUpdateCompleted = usePrevious(
    jobStatus?.lastCompletedTime?.unix(),
  );
  const lastUpdateCompletedUnixSecs = jobStatus?.lastCompletedTime?.unix();
  const lastUpdatedText = jobStatus?.lastCompletedTime?.format(
    "YYYY-MM-DD HH:mm:ss (UTC)" ?? "Never",
  );

  const triggerUpdateTableMetaJob = useCallback(
    async (onlyIfStale = true) => {
      const resp = await triggerUpdateTableMetaJobApi({ onlyIfStale });
      if (resp.job_triggered) {
        return refreshJobStatus();
      }
    },
    [refreshJobStatus],
  );

  const dataValidMs = jobStatus?.dataValidDuration.asMilliseconds();
  useEffect(() => {
    if (isLoading) {
      return;
    }
    // Schedule the next update request after the dataValidMs has passed since
    // the last update completed.
    const msSinceLastCompleted =
      Date.now() - lastUpdateCompletedUnixSecs * 1000;
    const delayMs = Math.max(0, dataValidMs - msSinceLastCompleted);
    const nextUpdated = setTimeout(() => {
      triggerUpdateTableMetaJob();
    }, delayMs);

    return () => clearTimeout(nextUpdated);
  }, [
    dataValidMs,
    lastUpdateCompletedUnixSecs,
    triggerUpdateTableMetaJob,
    isLoading,
  ]);

  useEffect(() => {
    // If the last completed time has changed, call the callback.
    if (previousUpdateCompleted === lastUpdateCompletedUnixSecs) {
      return;
    }
    onDataUpdated && onDataUpdated();
  }, [previousUpdateCompleted, lastUpdateCompletedUnixSecs, onDataUpdated]);

  const onRefreshClick = () => {
    // Force refresh.
    triggerUpdateTableMetaJob(false);
  };

  const isRunning = jobStatus?.currentStatus === TableMetadataJobStatus.RUNNING;
  return (
    <div className={styles["controls-container"]}>
      <Skeleton loading={isLoading}>
        <Tooltip
          title={
            "Data is last refreshed automatically (per cluster setting) or manually."
          }
        >
          Last refreshed: {lastUpdatedText}{" "}
        </Tooltip>
      </Skeleton>
      <Tooltip placement="top" title={"Refresh data"}>
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
