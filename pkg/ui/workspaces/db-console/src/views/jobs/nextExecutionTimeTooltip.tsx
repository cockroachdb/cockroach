// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Tooltip } from "antd";
import React from "react";
import { AbstractTooltipProps } from "antd/es/tooltip";
import { TimestampToMoment } from "src/util/convert";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { google } from "src/js/protos";

interface nextExecutionTimeTooltipProps extends AbstractTooltipProps {
  next_run: google.protobuf.ITimestamp;
  children?: React.ReactNode;
}

export const NextExecutionTimeTooltip = (
  props: nextExecutionTimeTooltipProps,
) => {
  const { next_run, children } = props;
  const nextRunMoment = moment(next_run.seconds.toNumber() * 1000);
  return (
    <Tooltip
      placement="bottom"
      title={`Next Execution Time: ${nextRunMoment.format(DATE_FORMAT)}`}
    >
      {children}
    </Tooltip>
  );
};
