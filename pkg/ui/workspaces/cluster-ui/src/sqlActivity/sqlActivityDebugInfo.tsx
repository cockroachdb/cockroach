// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Button, Text, Tooltip } from "@cockroachlabs/ui-components";

import { SqlStatsResponse } from "../api";

type Props = {
  info: SqlStatsResponse;
};

const SqlActivityDebugInfo = ({ info }: Props) => {
  const stmtsSource = info?.stmts_source_table || "Unset";
  const txnsSource = info?.txns_source_table || "Unset";
  const oldestAgg = info?.oldest_aggregated_ts_returned?.nanos
    ? new Date(info?.oldest_aggregated_ts_returned.nanos).toISOString()
    : "Unset";

  const content = info ? (
    <Text type="code">
      Statements Table: {stmtsSource}
      <br />
      Transactions Table: {txnsSource}
      <br />
      Oldest Aggregation: {oldestAgg}
    </Text>
  ) : (
    "Unset response."
  );
  return (
    <div>
      <Tooltip placement="top" content={content}>
        <Button intent="link-style">Debug Info</Button>
      </Tooltip>
    </div>
  );
};

export default SqlActivityDebugInfo;
