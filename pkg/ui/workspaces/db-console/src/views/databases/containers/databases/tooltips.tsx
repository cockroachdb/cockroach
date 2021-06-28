// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Tooltip } from "src/components";
import { TooltipProps } from "src/components/tooltip/tooltip";

export const TimeSeriesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Total disk size of Admin UI metrics.</p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const ReplicatedSizeTooltip: React.FC<
  TooltipProps & { tableName: string }
> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          Approximate disk size of all replicas of table{" "}
          {props.tableName || "<table name>"} on the cluster.
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);
