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
import { Alert, DatePicker, Form, Input, Popover, TimePicker } from "antd";
import { Moment } from "moment";
import classNames from "classnames/bind";
import { Time as TimeIcon, ErrorCircleFilled } from "@cockroachlabs/icons";
import { Button } from "src/button";
import { Text, TextTypes } from "src/text";

import styles from "./dateRange.module.scss";

const cx = classNames.bind(styles);

function rangeToString(start: Moment, end: Moment): string {
  const formatStr = "MMM D, H:mm";
  const formatStrSameDay = "H:mm";

  const isSameDay = start.isSame(end, "day");
  return `${start.utc().format(formatStr)} - ${end
    .utc()
    .format(isSameDay ? formatStrSameDay : formatStr)} (UTC)`;
}

type DateRangeMenuProps = {
  start: Moment;
  end: Moment;
  allowedInterval?: [Moment, Moment];
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
  const timeFormat = "H:mm [(UTC)]";
  const [startMoment, setStartMoment] = useState<Moment>(start);
  const [endMoment, setEndMoment] = useState<Moment>(end);

  const onChangeStart = (m?: Moment) => {
    m && setStartMoment(m);
  };

  const onChangeEnd = (m?: Moment) => {
    m && setEndMoment(m);
  };

  const isDisabled = allowedInterval
    ? (date: Moment): boolean => {
        return (
          date.isBefore(allowedInterval[0]) || date.isAfter(allowedInterval[1])
        );
      }
    : null;

  const isValid = startMoment.isBefore(endMoment);

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
        allowClear={false}
        format={dateFormat}
        onChange={onChangeStart}
        suffixIcon={<TimeIcon />}
        value={startMoment}
      />
      <TimePicker
        allowClear={false}
        format={timeFormat}
        onChange={onChangeStart}
        suffixIcon={<span />}
        value={startMoment}
      />
      <Text className={cx("label")} textType={TextTypes.CaptionStrong}>
        To
      </Text>
      <DatePicker
        allowClear={false}
        disabledDate={isDisabled}
        format={dateFormat}
        onChange={onChangeEnd}
        suffixIcon={<TimeIcon />}
        value={endMoment}
      />
      <TimePicker
        allowClear={false}
        format={timeFormat}
        onChange={onChangeEnd}
        suffixIcon={<span />}
        value={endMoment}
      />
      {!isValid && (
        <Alert
          icon={<ErrorCircleFilled fill="#FF3B4E" />}
          message="Date interval not valid"
          type="error"
          showIcon
        />
      )}
      <div className={cx("popup-footer")}>
        <Button onClick={closeMenu} type="secondary" textAlign="center">
          Cancel
        </Button>
        <Button
          disabled={!isValid}
          onClick={onApply}
          type="primary"
          textAlign="center"
        >
          Apply
        </Button>
      </div>
    </div>
  );
}

type DateRangeProps = {
  start: Moment;
  end: Moment;
  allowedInterval?: [Moment, Moment];
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
