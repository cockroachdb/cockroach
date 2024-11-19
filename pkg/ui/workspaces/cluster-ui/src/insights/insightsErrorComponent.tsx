// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import styles from "./workloadInsights/util/workloadInsights.module.scss";

const cx = classNames.bind(styles);

export const InsightsError = (errMsg?: string): React.ReactElement => {
  const message = errMsg
    ? errMsg
    : "This page had an unexpected error while loading insights.";
  const showReload = !message.toLowerCase().includes("size exceeded");
  return (
    <div className={cx("row")}>
      <span>{message}</span>
      &nbsp;
      {showReload && (
        <a
          className={cx("action")}
          onClick={() => {
            window.location.reload();
          }}
        >
          Reload this page
        </a>
      )}
    </div>
  );
};
