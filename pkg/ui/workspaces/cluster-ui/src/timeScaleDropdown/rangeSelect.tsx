// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState, useRef } from "react";
import { Button, Dropdown } from "antd";
import moment, { Moment } from "moment";
import { DateRangeMenu } from "src/dateRange";
import { CaretDown } from "src/icon/caretDown";
import classNames from "classnames/bind";

import styles from "./rangeSelector.module.scss";
import { TimeWindow } from "./timeScaleTypes";

const cx = classNames.bind(styles);

export type RangeOption = {
  value: string;
  label: string;
  timeLabel: string;
};

export type Selected = {
  dateStart?: string;
  dateEnd?: string;
  timeStart?: string;
  timeEnd?: string;
  title?: string;
  timeLabel?: string;
  timeWindow: TimeWindow;
};

interface RangeSelectProps {
  options: RangeOption[];
  onChange: (arg0: RangeOption) => void;
  changeDate: (dateRange: [moment.Moment, moment.Moment]) => void;
  onClosed?: () => void;
  selected: Selected;
}

type TimeLabelProps = {
  children: React.ReactNode;
};

const TimeLabel = ({ children }: TimeLabelProps) => {
  return <span className={cx("range__range-title")}>{children}</span>;
};

type OptionButtonProps = {
  option: RangeOption;
  isSelected: boolean;
  onClick: (option: RangeOption) => void;
};

const OptionButton = ({ option, onClick, isSelected }: OptionButtonProps) => {
  const _onClick = () => {
    onClick(option);
  };

  return (
    <Button
      type="default"
      className={cx("_time-button", isSelected ? "active" : "")}
      onClick={_onClick}
      ghost
    >
      <TimeLabel>
        {!isSelected && option.value === "Custom" ? "--" : option.timeLabel}
      </TimeLabel>
      <span className={cx("__option-label")}>
        {option.value === "Custom" ? "Custom date range" : option.value}
      </span>
    </Button>
  );
};

const RangeSelect = ({
  options,
  onChange,
  changeDate,
  onClosed,
  selected,
}: RangeSelectProps): React.ReactElement => {
  const [isVisible, setIsVisible] = useState<boolean>(false);
  const [custom, setCustom] = useState<boolean>(false);

  const rangeContainer = useRef<HTMLDivElement>();

  const onChangeDate = (start: Moment, end: Moment) => {
    closeDropdown();
    changeDate([start, end]);
  };

  const closeDropdown = () => {
    setCustom(false);
    setIsVisible(false);
    if (onClosed) {
      onClosed();
    }
  };

  const handleOptionButtonOnClick = (option: RangeOption): void => {
    if (option.value === "Custom") {
      setCustom(true);
      return;
    }
    closeDropdown();
    onChange(option);
  };

  const onVisibleChange = (visible: boolean): void => {
    setIsVisible(visible);
  };

  const menu = (
    <>
      {/**
       * isVisible is used here to trigger a remount of DateRangeMenu and re-initialize the time in the custom menu. See
       *  comments on DateRangeMenu.
       * It is needed because passing isVisible to <Dropdown> merely causes a css change in visibility, and does not
       *  re-mount the component.
       * The coupling of setting isVisible to true with the need to re-initialize the time relies on the implicit
       *  assumption that the dropdown is always in a closed state after a user-induced time change.
       */
      isVisible && (
        <div
          className={cx(
            "range-selector",
            `${custom ? "__custom" : "__options"}`,
          )}
        >
          {custom ? (
            <div className={cx("custom-menu")}>
              <DateRangeMenu
                startInit={selected.timeWindow.start}
                endInit={selected.timeWindow.end}
                onSubmit={onChangeDate}
                onCancel={() => setCustom(false)}
              />
            </div>
          ) : (
            <div className={cx("_quick-view")}>
              {options.map(option => (
                <OptionButton
                  key={option.label}
                  isSelected={selected.title === option.value}
                  option={option}
                  onClick={handleOptionButtonOnClick}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </>
  );

  return (
    <div ref={rangeContainer} className={cx("Range")}>
      <div className={cx("trigger-wrapper")}>
        <Dropdown
          visible={isVisible}
          onVisibleChange={onVisibleChange}
          placement="bottomLeft"
          trigger={["click"]}
          overlay={menu}
        >
          <Button className={cx("trigger-button")}>
            <div className={cx("trigger", "Select")}>
              <div>
                <TimeLabel>{selected.timeLabel}</TimeLabel>
                <span className={cx("Select-value-label", "title")}>
                  {selected.title !== "Custom" ? (
                    selected.title
                  ) : (
                    <>
                      {selected.dateStart}{" "}
                      <span className={cx("_label-time")}>
                        {selected.timeStart}
                      </span>{" "}
                      - {selected.dateEnd}{" "}
                      <span className={cx("_label-time")}>
                        {selected.timeEnd}
                      </span>{" "}
                      <span className={cx("Select-value-label__sufix")}>
                        (UTC)
                      </span>
                    </>
                  )}
                </span>
              </div>
              <span className={cx("caret-down")}>
                <CaretDown />
              </span>
            </div>
          </Button>
        </Dropdown>
      </div>
    </div>
  );
};

export default RangeSelect;
