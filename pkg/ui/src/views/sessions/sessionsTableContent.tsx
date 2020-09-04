// Copyright 2020 The Cockroach Authors.
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
import { Tooltip } from "src/components";
import styles from "./sessionsTableContent.module.styl";

const cx = classNames.bind(styles);

export const SessionTableTitle = {
  id: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Session ID."}
          </p>
        </div>
      }
    >
      Session ID
    </Tooltip>
  ),
  statement: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Most recent or currently active statement."}
          </p>
        </div>
      }
    >
      Statement
    </Tooltip>
  ),
  actions: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Actions to take on the session."}
          </p>
        </div>
      }
    >
      Actions
    </Tooltip>
  ),
  sessionAge: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The age of the session."}
          </p>
        </div>
      }
    >
      Session Age
    </Tooltip>
  ),
  txnAge: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The age of the open transaction, if there is one."}
          </p>
        </div>
      }
    >
      Txn Age
    </Tooltip>
  ),
  statementAge: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The age of the active statement, if there is one."}
          </p>
        </div>
      }
    >
      Query Age
    </Tooltip>
  ),
  memUsage: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The current amount of allocated memory on this session and the maximum amount of memory this" +
            " session has ever allocated."}
          </p>
        </div>
      }
    >
      Memory Usage
    </Tooltip>
  ),
  maxMemUsed: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The maximum amount of allocated memory this session ever had."}
          </p>
        </div>
      }
    >
      Maximum Memory Usage
    </Tooltip>
  ),
  numRetries: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The number of times this transaction encountered a retry."}
          </p>
        </div>
      }
    >
      Retries
    </Tooltip>
  ),
  lastActiveStatement: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The last statement that was completed on this session."}
          </p>
        </div>
      }
    >
      Last Statement
    </Tooltip>
  ),
  numStmts: (
    <Tooltip
      placement="bottom"
      title={
        <div className={cx("tooltip__table--title")}>
          <p>
            {"Number of statements that have been run in this transaction so far."}
          </p>
        </div>
      }
    >
      Statements Run
    </Tooltip>
  ),
};
