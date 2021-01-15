// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Button, notification, Icon } from "antd";
import moment, { Moment } from "moment";
import { TimeWindow } from "src/redux/timewindow";
import { trackTimeScaleSelected } from "src/util/analytics";
import React from "react";
import "./range.styl";
import { arrowRenderer } from "src/views/shared/components/dropdown";
import { RangeCalendar } from "src/components";

export enum DateTypes {
  DATE_FROM,
  DATE_TO,
}

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
};

interface RangeSelectProps {
  options: RangeOption[];
  onChange: (arg0: RangeOption) => void;
  changeDate: (dateRange: [moment.Moment, moment.Moment]) => void;
  onOpened?: () => void;
  onClosed?: () => void;
  value: TimeWindow;
  selected: Selected;
  useTimeRange: boolean;
}

type Nullable<T> = T | null;

interface RangeSelectState {
  opened: boolean;
  width: number;
  custom: boolean;
  selectMonthStart: Nullable<moment.Moment>;
  selectMonthEnd: Nullable<moment.Moment>;
}

class RangeSelect extends React.Component<RangeSelectProps, RangeSelectState> {
  state = {
    opened: false,
    width: window.innerWidth,
    custom: false,
    selectMonthStart: null as moment.Moment,
    selectMonthEnd: null as moment.Moment,
  };

  private rangeContainer = React.createRef<HTMLDivElement>();

  componentDidMount() {
    window.addEventListener("resize", this.updateDimensions);
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.updateDimensions);
  }

  updateDimensions = () => {
    this.setState({
      width: window.innerWidth,
    });
  };

  onChangeDate = (dateRange: [Moment, Moment]) => {
    const { changeDate } = this.props;
    this.toggleDropDown();
    changeDate(dateRange);
  };

  onChangeOption = (option: RangeOption) => () => {
    const { onChange } = this.props;
    this.toggleDropDown();
    onChange(option);
  };

  toggleCustomPicker = (custom: boolean) => () => {
    this.setState({ custom });
  };

  toggleDropDown = () => {
    this.setState(
      (prevState) => {
        return {
          opened: !prevState.opened,
          /*
          Always close the custom date picker pane when toggling the dropdown.

          The user must always manually choose to open it because right now we have
          no button to "go back" to the list of presets from the custom timepicker.
           */
          custom: false,
        };
      },
      () => {
        if (this.state.opened) {
          this.props.onOpened();
        } else {
          this.props.onClosed();
        }
      },
    );
  };

  handleOptionButtonOnClick = (option: RangeOption) => () => {
    trackTimeScaleSelected(option.label);
    (option.value === "Custom"
      ? this.toggleCustomPicker(true)
      : this.onChangeOption(option))();
  };

  optionButton = (option: RangeOption) => (
    <Button
      type="default"
      className={`_time-button ${
        (this.props.selected.title === option.value && "active") || ""
      }`}
      onClick={this.handleOptionButtonOnClick(option)}
      ghost
    >
      <span className="range__range-title">
        {this.props.selected.title !== "Custom" && option.value === "Custom"
          ? "--"
          : option.timeLabel}
      </span>
      <span className="__option-label">
        {option.value === "Custom" ? "Custom date range" : option.value}
      </span>
    </Button>
  );

  renderOptions = () => {
    const { options } = this.props;
    return options.map((option) => this.optionButton(option));
  };

  findSelectedValue = () => {
    const { options, selected } = this.props;
    const value = options.find((option) => option.value === selected.title);
    return value && value.label !== "Custom" ? (
      <span className="Select-value-label">{value.label}</span>
    ) : (
      <span className="Select-value-label">
        {selected.dateStart}{" "}
        <span className="_label-time">{selected.timeStart}</span> -{" "}
        {selected.dateEnd}{" "}
        <span className="_label-time">{selected.timeEnd}</span>{" "}
        <span className="Select-value-label__sufix">(UTC)</span>
      </span>
    );
  };

  headerRender = (item: any) => (
    <div className="calendar-month-picker">
      <Button
        type="default"
        onClick={() =>
          item.onChange(moment.utc(item.value).subtract(1, "months"))
        }
      >
        <Icon type="left" />
      </Button>
      <span>{moment.utc(item.value).format("MMM YYYY")}</span>
      <Button
        type="default"
        onClick={() => item.onChange(moment.utc(item.value).add(1, "months"))}
      >
        <Icon type="right" />
      </Button>
    </div>
  );

  renderContent = () => {
    const { custom } = this.state;

    if (!custom) {
      return <div className="_quick-view">{this.renderOptions()}</div>;
    }

    return (
      <RangeCalendar
        onSubmit={this.onChangeDate}
        onCancel={this.toggleDropDown}
        showBorders={false}
        onInvalidRangeSelect={() => {
          notification.info({
            message: "The timeframe has been set to a 10 minute range",
            description:
              "An invalid timeframe was entered. The timeframe has been set to a 10 minute range.",
          });
        }}
      />
    );
  };

  render() {
    const { opened, width, custom } = this.state;
    const selectedValue = this.findSelectedValue();
    const containerLeft = this.rangeContainer.current
      ? this.rangeContainer.current.getBoundingClientRect().left
      : 0;
    const left =
      width >= containerLeft + (custom ? 555 : 453)
        ? 0
        : width - (containerLeft + (custom ? 555 : 453));

    return (
      <div ref={this.rangeContainer} className="Range">
        <div className="click-zone" onClick={this.toggleDropDown} />
        {opened && (
          <div className="trigger-container" onClick={this.toggleDropDown} />
        )}
        <div className="trigger-wrapper">
          <div className={`trigger Select ${opened ? "is-open" : ""}`}>
            <span className="Select-value-label">{selectedValue}</span>
            <div className="Select-control">
              <div className="Select-arrow-zone">
                {arrowRenderer({ isOpen: opened })}
              </div>
            </div>
          </div>
          {opened && (
            <div
              className={`range-selector ${custom ? "__custom" : "__options"}`}
              style={{ left }}
            >
              {this.renderContent()}
            </div>
          )}
        </div>
      </div>
    );
  }
}

export default RangeSelect;
