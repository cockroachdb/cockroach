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
  start: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The timestamp at which the session started."}
    >
      Session Start Time (UTC)
    </Tooltip>
  ),
  sessionDuration: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The amount of time the session has been open."}
    >
      Session Duration
    </Tooltip>
  ),
  status: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={
        "A session is active if it has an open transaction (including implicit transactions, which are individual SQL statements), and idle if it has no open transaction."
      }
    >
      Status
    </Tooltip>
  ),
  mostRecentStatement: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={
        "If more than one statement is active, the most recent statement is shown. If the session is idle, the last statement is shown."
      }
    >
      Most Recent Statement
    </Tooltip>
  ),
  statementStartTime: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The timestamp at which the statement started."}
    >
      Statement Start Time (UTC)
    </Tooltip>
  ),
  memUsage: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={
        "Amount of memory currently allocated to this session, followed by the maximum amount of memory this session has ever been allocated."
      }
    >
      Memory Usage
    </Tooltip>
  ),
  clientAddress: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The IP address/port of the client that opened the session."}
    >
      Client IP Address
    </Tooltip>
  ),
  username: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The user that opened the session."}
    >
      User name
    </Tooltip>
  ),
  applicationName: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The application that ran the session."}
    >
      Application name
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
  txnAge: (
    <Tooltip
      style="tableTitle"
      placement="bottom"
      content={"The duration of the open transaction, if there is one."}
    >
      Transaction Duration
    </Tooltip>
  ),
};
