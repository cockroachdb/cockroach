// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Switch } from "antd";
import "antd/lib/switch/style";
import React from "react";
import classNames from "classnames/bind";
import styles from "./refreshControl.module.scss";
import RefreshIcon from "src/icon/refreshIcon";
import { Timestamp } from "src/timestamp";
import { Moment } from "moment-timezone";
import { DATE_WITH_SECONDS_FORMAT_24_TZ, capitalize } from "src/util";
import { ExecutionType } from "../types";

const cx = classNames.bind(styles);

interface RefreshControlProps {
  isAutoRefreshEnabled: boolean;
  onToggleAutoRefresh: () => void;
  onManualRefresh: () => void;
  lastRefreshTimestamp: Moment;
  execType: ExecutionType;
}

const REFRESH_BUTTON_COLOR = "#0055FF";

interface RefreshButtonProps {
  onManualRefresh: () => void;
}

// RefreshButton consists of the RefreshIcon and the text "Refresh".
const RefreshButton: React.FC<RefreshButtonProps> = ({ onManualRefresh }) => (
  <span className={cx("refresh-button")} onClick={onManualRefresh}>
    <RefreshIcon color={REFRESH_BUTTON_COLOR} className={cx("refresh-icon")} />
    <span className={cx("refresh-text")}>Refresh</span>
  </span>
);

export const RefreshControl: React.FC<RefreshControlProps> = ({
  isAutoRefreshEnabled,
  onToggleAutoRefresh,
  onManualRefresh,
  lastRefreshTimestamp,
  execType,
}) => {
  return (
    <div>
      <span className={cx("refresh-timestamp")}>
        <span>Active {capitalize(execType)} Executions As Of: </span>
        {lastRefreshTimestamp && lastRefreshTimestamp.isValid() ? (
          <Timestamp
            time={lastRefreshTimestamp}
            format={DATE_WITH_SECONDS_FORMAT_24_TZ}
          />
        ) : (
          "N/A"
        )}
      </span>
      <RefreshButton onManualRefresh={onManualRefresh} />
      <span className={cx("refresh-divider")}>
        <span className={cx("auto-refresh-label")}>Auto Refresh: </span>
        <Switch
          className={cx(`ant-switch-${isAutoRefreshEnabled ? "checked" : ""}`)}
          checkedChildren={"On"}
          unCheckedChildren={"Off"}
          checked={isAutoRefreshEnabled}
          onClick={onToggleAutoRefresh}
        />
      </span>
    </div>
  );
};

export default RefreshControl;
