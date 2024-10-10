// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { RedoOutlined } from "@ant-design/icons";
import { Skeleton, Tooltip } from "antd";
import React from "react";

import {
  TableMetadataJobStatus,
  TableMetaUpdateJobInfo,
  TriggerTableMetaUpdateJobResponse,
  triggerUpdateTableMetaJobApi,
} from "src/api/databases/tableMetaUpdateJobApi";
import Button from "src/sharedFromCloud/button";
import { Timestamp } from "src/timestamp";
import { DATE_WITH_SECONDS_FORMAT_24_TZ } from "src/util";

import styles from "./tableMetadataJobControl.module.scss";

type TableMetadataJobControlProps = {
  // Callback for when the job has updated the metadata, i.e. the
  // lastUpdatedTime has changed.
  error: string | null;
  jobStatus: TableMetaUpdateJobInfo | null;
  onJobTriggered?: (jobStatus: TriggerTableMetaUpdateJobResponse) => void;
};

export const TableMetadataJobControl: React.FC<
  TableMetadataJobControlProps
> = ({ jobStatus, onJobTriggered, error }) => {
  const onRefreshClick = async () => {
    // Force refresh.
    const resp = await triggerUpdateTableMetaJobApi({ onlyIfStale: false });
    if (resp.jobTriggered) {
      onJobTriggered && onJobTriggered(resp);
    }
  };

  const tooltipText = error
    ? error
    : "Data is last refreshed automatically (per cluster setting) or manually.";

  const durationText = jobStatus?.lastCompletedTime?.fromNow();
  const isRunning = jobStatus?.currentStatus === TableMetadataJobStatus.RUNNING;
  const refreshTooltip = isRunning ? "Data is being refreshed" : "Refresh data";

  return (
    <div className={styles["controls-container"]}>
      <Skeleton loading={jobStatus == null}>
        <Tooltip title={tooltipText}>
          Last refreshed:
          <Timestamp
            format={DATE_WITH_SECONDS_FORMAT_24_TZ}
            time={jobStatus?.lastCompletedTime}
            fallback={"Never"}
          />{" "}
          {durationText && `(${durationText})`}
        </Tooltip>
      </Skeleton>
      <Tooltip placement="top" title={refreshTooltip}>
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
