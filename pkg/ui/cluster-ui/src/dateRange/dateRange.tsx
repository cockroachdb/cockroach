// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import { DatePicker, Form, Input, Popover, TimePicker } from "antd";
import moment, { Moment } from "moment";
// import { InputProps } from "antd/lib/input";
import classNames from "classnames/bind";
import { Time as TimeIcon } from "@cockroachlabs/icons";
import { Button } from "src/button";
import { Text, TextTypes } from "src/text";

import styles from "./dateRange.module.scss";

const cx = classNames.bind(styles);

function rangeToString(start: Moment, end: Moment): string {
  const formatStr = "MMM D, h:mm A";
  return `${start.utc().format(formatStr)} - ${end
    .utc()
    .format(formatStr)} (UTC)`;
}

type DateRangeMenuProps = {
  start: Moment;
  end: Moment;
  allowedInterval: [Moment, Moment];
  closeMenu: () => void;
  onSubmit: (start: Moment, end: Moment) => void;
};

function DateRangeMenu({
  start,
  end,
  allowedInterval,
  closeMenu,
  onSubmit,
}: DateRangeMenuProps): React.ReactElement {
  const dateFormat = "MMMM D, YYYY";
  const timeFormat = "h:mm A [(UTC)]";
  const minDate = allowedInterval[0];
  const maxDate = allowedInterval[1];
  const [startMoment, setStartMoment] = useState<Moment>(start);
  const [endMoment, setEndMoment] = useState<Moment>(end);

  const isDisabled = (date: Moment): boolean => {
    return date.isBefore(minDate) || date.isAfter(maxDate);
  };

  const onApply = (): void => {
    onSubmit(startMoment, endMoment);
    closeMenu();
  };

  return (
    <div className={cx("popup-content")}>
      <Text className={cx("label")} textType={TextTypes.CaptionStrong}>
        From
      </Text>
      <DatePicker
        disabledDate={isDisabled}
        format={dateFormat}
        onChange={setStartMoment}
        suffixIcon={<TimeIcon />}
        value={startMoment}
      />
      <TimePicker
        format={timeFormat}
        onChange={setStartMoment}
        suffixIcon={<span />}
        use12Hours
        value={startMoment}
      />
      <Text className={cx("label")} textType={TextTypes.CaptionStrong}>
        To
      </Text>
      <DatePicker
        disabledDate={isDisabled}
        format={dateFormat}
        onChange={setEndMoment}
        suffixIcon={<TimeIcon />}
        value={endMoment}
      />
      <TimePicker
        format={timeFormat}
        onChange={setEndMoment}
        suffixIcon={<span />}
        use12Hours
        value={endMoment}
      />
      <div className={cx("popup-footer")}>
        <Button onClick={closeMenu} type="secondary" textAlign="center">
          Cancel
        </Button>
        <Button onClick={onApply} type="primary" textAlign="center">
          Apply
        </Button>
      </div>
    </div>
  );
}

type DateRangeProps = {
  start: Moment;
  end: Moment;
  allowedInterval: [Moment, Moment];
  onSubmit: (start: Moment, end: Moment) => void;
};

export function DateRange({
  allowedInterval,
  start,
  end,
  onSubmit,
}: DateRangeProps): React.ReactElement {
  const [menuVisible, setMenuVisible] = useState<boolean>(false);
  const displayStr = rangeToString(start, end);

  const onVisibleChange = (visible: boolean): void => {
    setMenuVisible(visible);
  };

  const closeMenu = (): void => {
    setMenuVisible(false);
  };

  const menu = (
    <DateRangeMenu
      allowedInterval={allowedInterval}
      start={start}
      end={end}
      closeMenu={closeMenu}
      onSubmit={onSubmit}
    />
  );

  return (
    <Form className={cx("date-range-form")}>
      <Form.Item>
        <Popover
          destroyTooltipOnHide
          content={menu}
          overlayClassName={cx("popup-container")}
          placement="bottomLeft"
          visible={menuVisible}
          onVisibleChange={onVisibleChange}
          trigger="click"
        >
          <Input value={displayStr} prefix={<TimeIcon />} />
        </Popover>
      </Form.Item>
    </Form>
  );
}
