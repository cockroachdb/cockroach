// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useCallback, useMemo, useState } from "react";
import moment, { DurationInputArg1, DurationInputArg2, Moment } from "moment";
import { noop } from "lodash";
import { TimePicker } from "antd";
import { TimePickerProps } from "antd/es/time-picker";
import RcRangeCalendar from "rc-calendar/es/RangeCalendar";
import locale from "rc-calendar/es/locale/en_US";
import "rc-calendar/assets/index.css";
import classNames from "classnames/bind";
import { CaretDown } from "@cockroachlabs/icons";

import { Button } from "src/components";
import styles from "./rangeCalendar.module.styl";
import { DateRangeLabel } from "./dateRangeLabel";

const cx = classNames.bind(styles);

locale.monthFormat = "MMMM";

interface OwnProps {
  timeFormat?: string;
  minTimeRange?: [DurationInputArg1, DurationInputArg2];
  onSubmit?: (range: [Moment, Moment]) => void;
  onCancel?: () => void;
  onInvalidRangeSelect?: (
    allowedRange: [DurationInputArg1, DurationInputArg2],
  ) => void;
  showBorders?: boolean;
}

interface RcTimePickerProps {
  align: {
    offset: [number, number];
  };
}

export type RangeCalendarProps = OwnProps;

function isBelowMinDateRange(
  minTimeRange: [DurationInputArg1, DurationInputArg2],
  from: Moment,
  to: Moment,
): boolean {
  const minutesInRange = Math.abs(from.diff(to, minTimeRange[1], true));
  return minutesInRange < minTimeRange[0];
}

export const RangeCalendar: React.FC<RangeCalendarProps> = ({
  timeFormat = "h:mm A",
  minTimeRange = [10, "minute"],
  onCancel = noop,
  onSubmit = noop,
  onInvalidRangeSelect = noop,
  showBorders = true,
}) => {
  const currentDate = useMemo<Moment>(() => moment.utc().startOf("day"), []);
  const [startDate, setStartDate] = useState<Moment>(currentDate);
  const [endDate, setEndDate] = useState<Moment>(
    moment.utc(currentDate).endOf("day"),
  );

  const onDateRangeSelected = useCallback(
    ([selectedStartDate, selectedEndDate]: [Moment | null, Moment | null]) => {
      // Set only Date part and keep Time part unchanged
      setStartDate((prevDate) =>
        moment.utc(prevDate).set({
          year: selectedStartDate.year(),
          month: selectedStartDate.month(),
          date: selectedStartDate.date(),
        }),
      );

      setEndDate((prevDate) =>
        moment.utc(prevDate).set({
          year: selectedEndDate.year(),
          month: selectedEndDate.month(),
          date: selectedEndDate.date(),
        }),
      );

      // If date range is lower than allowed, update end date to be
      // equal startDate + min allowed date range
      if (
        isBelowMinDateRange(minTimeRange, selectedStartDate, selectedEndDate)
      ) {
        setEndDate(moment.utc(selectedStartDate).add(...minTimeRange));
        onInvalidRangeSelect(minTimeRange);
      }
    },
    [minTimeRange, onInvalidRangeSelect],
  );

  const onTimeChange = useCallback(
    (rangePart: "from" | "to") => (nextTime: Moment) => {
      const setDate = rangePart === "from" ? setStartDate : setEndDate;
      const setOtherDate = rangePart !== "from" ? setStartDate : setEndDate;
      const otherDate = rangePart === "from" ? endDate : startDate;

      setDate((prevDate) =>
        moment.utc(prevDate).set({
          hour: nextTime.get("hour"),
          minute: nextTime.get("minute"),
          second: nextTime.get("second"),
        }),
      );

      // Handle edge case when `startDate` and `endDate` is the same day, In this case,
      // if `startTime` > `endTime` then endTime = startTime + 10min
      // if `endTime` < `startTime` then startTime = endTime - 10min
      // Keep new value for changed time part and update opposite part
      // automatically to keep at least 10min range.
      if (isBelowMinDateRange(minTimeRange, nextTime, otherDate)) {
        const date =
          rangePart === "from"
            ? nextTime.add(...minTimeRange)
            : nextTime.subtract(...minTimeRange);

        setOtherDate(date);
        onInvalidRangeSelect(minTimeRange);
      }
    },
    [endDate, startDate, minTimeRange, onInvalidRangeSelect],
  );

  const onSubmitClick = useCallback(() => onSubmit([startDate, endDate]), [
    onSubmit,
    startDate,
    endDate,
  ]);

  const timePickerDefaultProps: TimePickerProps & RcTimePickerProps = {
    allowClear: false,
    use12Hours: true,
    inputReadOnly: true,
    format: timeFormat,
    className: cx("crl-time-picker"),
    popupClassName: cx("crl-time-picker-popup"),
    defaultValue: currentDate,
    suffixIcon: <CaretDown fontSize={14} />,
    placeholder: "",
    // Reposition popup to avoid overlapping it on top
    // of timepicker input.
    align: {
      offset: [4, 41],
    },
  };

  const renderFooter = () => {
    return (
      <div className={cx("crl-footer")}>
        <div className={cx("crl-time-picker-container")}>
          <div>
            <TimePicker
              {...timePickerDefaultProps}
              onChange={onTimeChange("from")}
              value={startDate}
            />
          </div>
          <div>
            <TimePicker
              {...timePickerDefaultProps}
              onChange={onTimeChange("to")}
              value={endDate}
            />
          </div>
        </div>
        <div className={cx("")}>
          <div className={cx("crl-date-preview")}>
            <DateRangeLabel from={startDate} to={endDate} />
          </div>
          <div className={cx("crl-action-buttons")}>
            <Button onClick={onCancel} type="secondary">
              Cancel
            </Button>
            <Button onClick={onSubmitClick}>Apply</Button>
          </div>
        </div>
      </div>
    );
  };

  return (
    <RcRangeCalendar
      mode={["date", "date"]}
      defaultSelectedValue={[startDate, endDate]}
      selectedValue={[startDate, endDate]}
      locale={locale}
      showToday={false}
      showDateInput={false}
      seperator=""
      renderFooter={renderFooter}
      className={cx("crl-calendar", {
        "crl-calendar__no-borders": !showBorders,
      })}
      onSelect={onDateRangeSelected}
    />
  );
};
