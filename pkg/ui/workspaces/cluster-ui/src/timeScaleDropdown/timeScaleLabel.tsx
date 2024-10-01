// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Icon } from "@cockroachlabs/ui-components";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useContext } from "react";

import { TimezoneContext } from "src/contexts/timezoneContext";
import { Timezone } from "src/timestamp";

import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";

import { FormattedTimescale } from "./formattedTimeScale";
import { dateFormat, timeFormat } from "./timeScaleDropdown";
import { TimeScale } from "./timeScaleTypes";
import { toRoundedDateRange } from "./utils";

const timeScaleStylesCx = classNames.bind(timeScaleStyles);

interface TimeScaleLabelProps {
  timeScale: TimeScale;
  requestTime: moment.Moment;
  oldestDataTime?: moment.Moment;
}

export const TimeScaleLabel: React.FC<TimeScaleLabelProps> = ({
  timeScale,
  requestTime,
  oldestDataTime,
}): React.ReactElement => {
  const period = (
    <FormattedTimescale ts={timeScale} requestTime={moment(requestTime)} />
  );
  const label = (
    <>
      Showing aggregated stats from{" "}
      <span className={timeScaleStylesCx("bold")}>{period}</span>
    </>
  );

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [start, _] = toRoundedDateRange(timeScale);
  const showWarning = oldestDataTime && oldestDataTime.diff(start, "hours") > 1;
  const timezone = useContext(TimezoneContext);
  const oldestTz = moment(oldestDataTime)?.tz(timezone);
  const oldestLabel = (
    <>
      {`SQL Stats are available since ${oldestTz?.format(
        dateFormat,
      )} ${oldestTz?.format(timeFormat)} `}
      <Timezone />
    </>
  );

  const warning = (
    <Tooltip placement="bottom" title={oldestLabel}>
      <Icon
        iconName="Caution"
        fill="warning"
        className={timeScaleStylesCx("warning-icon-area")}
      />
    </Tooltip>
  );

  return (
    <>
      {showWarning && warning} {label}
    </>
  );
};
