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
import { Tooltip2 } from "../tooltip2";

export const SessionTableTitle = {
  id: (
    <Tooltip2 tableTitle placement="bottom" title={<>{"Session ID."}</>}>
      Session ID
    </Tooltip2>
  ),
  statement: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"Most recent or currently active statement."}</>}
    >
      Statement
    </Tooltip2>
  ),
  actions: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"Actions to take on the session."}</>}
    >
      Actions
    </Tooltip2>
  ),
  sessionAge: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"The duration of the session."}</>}
    >
      Session Duration
    </Tooltip2>
  ),
  txnAge: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"The duration of the open transaction, if there is one."}</>}
    >
      Transaction Duration
    </Tooltip2>
  ),
  statementAge: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"The duration of the active statement, if there is one."}</>}
    >
      Statement Duration
    </Tooltip2>
  ),
  memUsage: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={
        <>
          {"The current amount of allocated memory on this session and the maximum amount of memory this" +
            " session has ever allocated."}
        </>
      }
    >
      Memory Usage
    </Tooltip2>
  ),
  maxMemUsed: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={
        <>{"The maximum amount of allocated memory this session ever had."}</>
      }
    >
      Maximum Memory Usage
    </Tooltip2>
  ),
  numRetries: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"The number of times this transaction encountered a retry."}</>}
    >
      Retries
    </Tooltip2>
  ),
  lastActiveStatement: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={<>{"The last statement that was completed on this session."}</>}
    >
      Last Statement
    </Tooltip2>
  ),
  numStmts: (
    <Tooltip2
      tableTitle
      placement="bottom"
      title={
        <>
          {
            "Number of statements that have been run in this transaction so far."
          }
        </>
      }
    >
      Statements Run
    </Tooltip2>
  ),
};
