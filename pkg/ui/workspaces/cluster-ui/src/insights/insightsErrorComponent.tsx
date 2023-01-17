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

// Error messages relating to upgrades in progress.
// This is a temporary solution until we can use different queries for
// different versions. For now we just try to give more info as to why
// this page is unavailable for insights.
const UPGRADE_RELATED_ERRORS = [
  /relation "crdb_internal.txn_execution_insights" does not exist/i,
  /column "(.*)" does not exist/i,
];

function isUpgradeError(message: string): boolean {
  return UPGRADE_RELATED_ERRORS.some(err => message.search(err));
}

function createInsightsErrMessage(
  errMsg: string | null,
  isUpgradeErr: boolean,
): string {
  const base = "This page had an unexpected error while loading insights.";

  if (isUpgradeErr) {
    return `${base} If your cluster is upgrading, please note that this page may not be available until upgrade is complete.`;
  }

  return errMsg ?? base;
}

export const InsightsError = (errMsg?: string): React.ReactElement => {
  const isUpgradeErr = isUpgradeError(errMsg);

  const message = createInsightsErrMessage(errMsg, isUpgradeErr);

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
