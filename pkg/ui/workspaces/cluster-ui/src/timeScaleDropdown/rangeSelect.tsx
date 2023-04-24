// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState, useRef, useContext } from "react";
import { Button, Dropdown } from "antd";
import "antd/lib/button/style";
import "antd/lib/dropdown/style";
import moment, { Moment } from "moment-timezone";
import { DateRangeMenu } from "src/dateRangeMenu";
import { CaretDown } from "src/icon/caretDown";
import classNames from "classnames/bind";

import styles from "./rangeSelector.module.scss";
import { TimeWindow } from "./timeScaleTypes";
import { TimezoneContext } from "../contexts";
import { Timezone } from "src/timestamp";

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
  key: "Custom" | string;
  timeLabel: string;
  timeWindow: TimeWindow;
};

interface RangeSelectProps {
  options: RangeOption[];
  onPresetOptionSelect: (arg0: RangeOption) => void;
  onCustomSelect: (dateRange: [moment.Moment, moment.Moment]) => void;
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
        {option.value === "Custom" ? "Custom time interval" : option.value}
      </span>
    </Button>
  );
};

const RangeSelect = ({
  options,
  onPresetOptionSelect,
  onCustomSelect,
  selected,
}: RangeSelectProps): React.ReactElement => {
  const [isVisible, setIsVisible] = useState<boolean>(false);
  const timezone = useContext(TimezoneContext);
  /**
   * customDropdownOptionWasJustSelected holds whether the user had just clicked the "Custom time interval" option in
   * the dropdown menu.
   * It is NOT whether the user had just selected a custom time by clicking "Apply".
   */
  const [
    customDropdownOptionWasJustSelected,
    setCustomDropdownOptionWasJustSelected,
  ] = useState<boolean>(false);

  /**
   * customBackWasJustSelected holds whether the "Back", as in back to preset options, button in the custom menu was
   * just selected.
   */
  const [customBackWasJustSelected, setReturnToPresetOptionsWasJustSelected] =
    useState<boolean>(false);

  const rangeContainer = useRef<HTMLDivElement>();

  const handleEvent = (
    eventIsSelectingCustomDropdownOption: boolean,
    eventIsReturnToPresetOptions: boolean,
  ) => {
    setCustomDropdownOptionWasJustSelected(
      eventIsSelectingCustomDropdownOption,
    );
    setReturnToPresetOptionsWasJustSelected(eventIsReturnToPresetOptions);
  };

  const onVisibleChange = (visible: boolean): void => {
    handleEvent(false, false);
    setIsVisible(visible);
  };

  const closeDropdown = () => {
    onVisibleChange(false);
  };

  const onDropdownOptionClick = (option: RangeOption): void => {
    if (option.value === "Custom") {
      // Switch to showing the DateRangeMenu, for users to select a custom time. The dropdown remains open.
      handleEvent(true, false);
      return;
    }
    onPresetOptionSelect(option);
    closeDropdown();
  };

  const onCustomSelectWrapper = (start: Moment, end: Moment) => {
    onCustomSelect([start, end]);
    closeDropdown();
  };

  const onReturnToPresetOptionsClick = () => {
    handleEvent(false, true);
  };

  const selectedIsCustom = selected.key === "Custom";
  const shouldShowCustom =
    customDropdownOptionWasJustSelected ||
    (selectedIsCustom && !customBackWasJustSelected);

  const menu = (
    <>
      {
        /**
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
              `${shouldShowCustom ? "__custom" : "__options"}`,
            )}
          >
            {shouldShowCustom ? (
              <div className={cx("custom-menu")}>
                <DateRangeMenu
                  startInit={selected.timeWindow.start}
                  endInit={selected.timeWindow.end}
                  onSubmit={onCustomSelectWrapper}
                  onCancel={closeDropdown}
                  onReturnToPresetOptionsClick={onReturnToPresetOptionsClick}
                />
              </div>
            ) : (
              <div className={cx("_quick-view")}>
                {options.map(option => (
                  <OptionButton
                    key={option.label}
                    isSelected={selected.key === option.value}
                    option={option}
                    onClick={onDropdownOptionClick}
                  />
                ))}
              </div>
            )}
          </div>
        )
      }
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
                  {!selectedIsCustom ? (
                    selected.key
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
                        <Timezone />
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
