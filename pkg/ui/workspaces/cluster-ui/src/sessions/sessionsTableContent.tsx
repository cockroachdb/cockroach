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
import { Tooltip } from "@cockroachlabs/ui-components";

export const SessionTableTitle = {
  id: (
    <Tooltip style="tableTitle" placement="bottom" content={"Session ID."}>
      Session ID
    </Tooltip>
  ),
  statement: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"Most recent or currently active statement."}
    >
      Statement
    </Tooltip>
  ),
  actions: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"Actions to take on the session."}
    >
      Actions
    </Tooltip>
  ),
  sessionAge: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The duration of the session."}
    >
      Session Duration
    </Tooltip>
  ),
  txnAge: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The duration of the open transaction, if there is one."}
    >
      Transaction Duration
    </Tooltip>
  ),
  statementAge: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The duration of the active statement, if there is one."}
    >
      Statement Duration
    </Tooltip>
  ),
  memUsage: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={
        "The current amount of allocated memory on this session and the maximum amount of memory this" +
        " session has ever allocated."
      }
    >
      Memory Usage
    </Tooltip>
  ),
  maxMemUsed: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The maximum amount of allocated memory this session ever had."}
    >
      Maximum Memory Usage
    </Tooltip>
  ),
  numRetries: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The number of times this transaction encountered a retry."}
    >
      Retries
    </Tooltip>
  ),
  lastActiveStatement: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The last statement that was completed on this session."}
    >
      Last Statement
    </Tooltip>
  ),
  numStmts: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={
        "Number of statements that have been run in this transaction so far."
      }
    >
      Statements Run
    </Tooltip>
  ),
};
