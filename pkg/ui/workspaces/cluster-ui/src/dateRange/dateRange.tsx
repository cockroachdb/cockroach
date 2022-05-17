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
import moment, { Moment } from "moment";
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
  startInit?: Moment;
  endInit?: Moment;
  allowedInterval?: [Moment, Moment];
  onSubmit: (start: Moment, end: Moment) => void;
  onCancel: () => void;
};

export const dateFormat = "MMMM D, YYYY";
export const timeFormat = "H:mm";

export function DateRangeMenu({
  startInit,
  endInit,
  allowedInterval,
  onSubmit,
  onCancel,
}: DateRangeMenuProps): React.ReactElement {
  /**
   * Local startMoment and endMoment state are stored here so that users can change the time before clicking "Apply".
   * They are re-initialized to startInit and endInit by re-mounting this component. It is thus the responsibility of
   *  consuming parent components to re-mount this component when the time needs to be re-initialized (e.g., after any
   *  user action changes the selected time).
   *
   * This re-initialization is done by re-mounting instead of a useEffect because when one of the preset, non-custom
   *  "Past ___ time" options is selected, the parent components <TimeScaleDropdown> -> <RangeSelect> pass startInit
   *  and endInit to this component as derived from calling now(). now() called on every <TimeScaleDropdown> render.
   * startInit and endInit props will thus change over time through no input of the user when a non-custom, "Past ___ time"
   *  option is selected. This can cause a bug where, as startInit updates over time, the time picker selection reverts
   *  to this updated time in the middle of while the user is attempting to change the selection. One can imagine a
   *  situation where the users goes to modify the start time, and in the meantime the end time changes.
   *
   * It would be ideal if consuming parent components could pass startInit and endInit that only changed onuser input.
   * However, this is difficult on Statement and Transaction pgaes because polling is done through an implicit throttle
   *  on CachedDataReducer, and the "actual" time is thus not available.
   *
   * Going with preferring a stale initial time over one that updates too aggressively, the responsibility is thus on
   *  the parent component to re-initialize this.
   */
  const [startMoment, setStartMoment] = useState<Moment>(
    startInit || moment.utc(),
  );
  const [endMoment, setEndMoment] = useState<Moment>(endInit || moment.utc());

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

  let errorMessage;
  if (startMoment.isAfter(endMoment)) {
    errorMessage = "Select an end time that is after the start time.";
  } else if (
    // Add time to current timestamp to account for delays on requests
    startMoment.isAfter(moment().add(5, "minutes")) ||
    endMoment.isAfter(moment().add(5, "minutes"))
  ) {
    errorMessage = "Select a date and time that is not in the future.";
  }
  const isValid = errorMessage === undefined;

  const onApply = (): void => {
    onSubmit(startMoment, endMoment);
  };

  return (
    <div className={cx("popup-content")}>
      <Text className={cx("label")} textType={TextTypes.BodyStrong}>
        Start (UTC)
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
      <div className={cx("divider")} />
      <Text className={cx("label")} textType={TextTypes.BodyStrong}>
        End (UTC)
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
          message={errorMessage}
          type="error"
          showIcon
        />
      )}
      <div className={cx("popup-footer")}>
        <Button onClick={onCancel} type="secondary" textAlign="center">
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

  const _onSubmit = (start: Moment, end: Moment) => {
    onSubmit(start, end);
    closeMenu();
  };

  const menu = (
    <DateRangeMenu
      allowedInterval={allowedInterval}
      startInit={start}
      endInit={end}
      onSubmit={_onSubmit}
      onCancel={closeMenu}
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
