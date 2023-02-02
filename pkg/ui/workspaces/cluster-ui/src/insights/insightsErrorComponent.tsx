// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
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
