// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Button, DatePicker, TimePicker, notification } from "antd";
import moment, { Moment } from "moment";
import { TimeWindow } from "oss/src/redux/timewindow";
import React from "react";
import "./range.styl";

export enum DateTypes {
  DATE_FROM,
  DATE_TO,
}

type RangeOption = {
  value: string;
  label: string;
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
  changeDate: (arg0: moment.Moment, arg1: DateTypes) => void;
  value: TimeWindow;
  selected: Selected;
  useTimeRange: boolean;
}
interface RangeSelectState {
  opened: boolean;
  width: number;
}

class RangeSelect extends React.Component<RangeSelectProps, RangeSelectState> {
  state = {
    opened: false,
    width: window.innerWidth,
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
  }

  isValid = (date: moment.Moment, direction: DateTypes) => {
    const { value } = this.props;
    let valid = true;
    switch (direction) {
      case DateTypes.DATE_FROM:
        valid = !(date >= value.end);
        break;
      case DateTypes.DATE_TO:
        valid = !(date <= value.start);
        break;
      default:
        valid = true;
    }
    return valid;
  }

  onChangeDate = (direction: DateTypes) => (date: Moment) => {
    const { changeDate, value } = this.props;
    if (this.isValid(date, direction)) {
      changeDate(moment.utc(date), direction);
    } else {
      if (direction === DateTypes.DATE_TO) {
        changeDate(moment(value.start).add(10, "minute"), direction);
      } else {
        changeDate(moment(value.end).add(-10, "minute"), direction);
      }
      notification.info({
        message: "The timeframe has been set to a 10 minute range",
        description: "An invalid timeframe was entered. The timeframe has been set to a 10 minute range.",
      });
    }
  }

  renderTimePickerAddon = (direction: DateTypes) => () => <Button onClick={() => this.onChangeDate(direction)(moment())} type="default" size="small">Now</Button>;

  renderDatePickerAddon = (direction: DateTypes) => () => <Button onClick={() => this.onChangeDate(direction)(moment())} type="default" size="small">Today</Button>;

  onChangeOption = (option: RangeOption) => () => {
    const { onChange } = this.props;
    this.setState({ opened: false });
    onChange(option);
  }

  renderOptions = () => {
    const { options, selected } = this.props;
    return options.map(option => option.label !== "Custom" && (
      <Button
        className={`_time-button ${selected.title === option.value && "active" || ""}`}
        onClick={this.onChangeOption(option)}
        type="default"
        ghost
      >
        {option.value}
      </Button>
    ));
  }

  findSelectedValue = () => {
    const { options, selected } = this.props;
    const value = options.find(option => option.value === selected.title);
    return value ? (
      <span className="Select-value-label">{value.label}</span>
    ) : (
      <span className="Select-value-label">{selected.dateStart} <span className="_label-time">{selected.timeStart}</span> - {selected.dateEnd} <span className="_label-time">{selected.timeEnd}</span></span>
    );
  }

  getDisabledHours = (isStart?: boolean) => () => {
    const { value } = this.props;
    const start = Number(moment.utc(value.start).format("HH"));
    const end = Number(moment.utc(value.end).format("HH"));
    const hours = [];
    for (let i = 0 ; i < (isStart ? moment().hour() : start); i++) {
      if (isStart) {
        hours.push((end + 1) + i);
      } else {
        hours.push(i);
      }
    }
    return hours;
  }

  getDisabledMinutes = (isStart?: boolean) => () => {
    const { value } = this.props;
    const startHour = Number(moment.utc(value.start).format("HH"));
    const endHour = Number(moment.utc(value.end).format("HH"));
    const startMinutes = Number(moment.utc(value.start).format("mm"));
    const endMinutes = Number(moment.utc(value.end).format("mm"));
    const minutes = [];
    if (startHour === endHour) {
      for (let i = 0 ; i < (isStart ? moment().minute() : startMinutes); i++) {
        if (isStart) {
          minutes.push((endMinutes + 1) + i);
        } else {
          minutes.push(i);
        }
      }
    }
    return minutes;
  }

  getDisabledSeconds = (isStart?: boolean) => () => {
    const { value } = this.props;
    const startHour = Number(moment.utc(value.start).format("HH"));
    const endHour = Number(moment.utc(value.end).format("HH"));
    const startMinutes = Number(moment.utc(value.start).format("mm"));
    const endMinutes = Number(moment.utc(value.end).format("mm"));
    const startSeconds = Number(moment.utc(value.start).format("ss"));
    const endSeconds = Number(moment.utc(value.end).format("ss"));
    const seconds = [];
    if (startHour === endHour && startMinutes === endMinutes) {
      for (let i = 0 ; i < (isStart ? moment().second() : startSeconds + 1); i++) {
        if (isStart) {
          seconds.push(endSeconds + i);
        } else {
          seconds.push(i);
        }
      }
    }
    return seconds;
  }

  render() {
    const { value, useTimeRange } = this.props;
    const { opened, width } = this.state;
    const start = useTimeRange ? moment.utc(value.start) : null;
    const end = useTimeRange ? moment.utc(value.end) : null;
    const datePickerFormat = "M/DD/YYYY";
    const timePickerFormat = "h:mm:ss A";
    const selectedValue = this.findSelectedValue();
    const isSameDate = useTimeRange && moment(start).isSame(end, "day");
    const containerLeft = this.rangeContainer.current ? this.rangeContainer.current.getBoundingClientRect().left : 0;
    const left = width >= (containerLeft + 500) ? 0 : width - (containerLeft + 500);
    const content = (
      <div className="range-selector" style={{ left }}>
        <div className="_quick-view">
          <span className="_title">Quick view</span>
          {this.renderOptions()}
        </div>
        <div className="_start">
          <span className="_title">Start</span>
          <DatePicker
            dropdownClassName="disabled-year"
            value={start}
            disabledDate={(currentDate) => (currentDate > (end || moment()))}
            allowClear={false}
            format={`${datePickerFormat} ${moment(start).isSame(moment.utc(), "day") && "[- Today]" || ""}`}
            onChange={this.onChangeDate(DateTypes.DATE_FROM)}
            renderExtraFooter={this.renderDatePickerAddon(DateTypes.DATE_FROM)}
            showToday={false}
          />
          <TimePicker
            value={start}
            format={`${timePickerFormat} ${moment(start).isSame(moment.utc(), "minute") && "[- Now]" || ""}`}
            use12Hours
            addon={this.renderTimePickerAddon(DateTypes.DATE_FROM)}
            onChange={this.onChangeDate(DateTypes.DATE_FROM)}
            disabledHours={isSameDate && this.getDisabledHours(true) || undefined}
            disabledMinutes={isSameDate && this.getDisabledMinutes(true) || undefined}
            disabledSeconds={isSameDate && this.getDisabledSeconds(true) || undefined}
          />
        </div>
        <div className="_end">
          <span className="_title">End</span>
          <DatePicker
            dropdownClassName="disabled-year"
            value={end}
            disabledDate={(currentDate) => (currentDate > moment() || currentDate < (start || moment()))}
            allowClear={false}
            format={`${datePickerFormat} ${moment(end).isSame(moment.utc(), "day") && "[- Today]" || ""}`}
            onChange={this.onChangeDate(DateTypes.DATE_TO)}
            renderExtraFooter={this.renderDatePickerAddon(DateTypes.DATE_TO)}
            showToday={false}
          />
          <TimePicker
            value={end}
            format={`${timePickerFormat} ${moment(end).isSame(moment.utc(), "minute") && "[- Now]" || ""}`}
            use12Hours
            addon={this.renderTimePickerAddon(DateTypes.DATE_TO)}
            onChange={this.onChangeDate(DateTypes.DATE_TO)}
            disabledHours={isSameDate && this.getDisabledHours() || undefined}
            disabledMinutes={isSameDate && this.getDisabledMinutes() || undefined}
            disabledSeconds={isSameDate && this.getDisabledSeconds() || undefined}
          />
        </div>
      </div>
    );

    return (
      <div ref={this.rangeContainer} className="Range">
        <div className="click-zone" onClick={() => this.setState({ opened: !opened })}/>
        {opened && <div className="trigger-container" onClick={() => this.setState({ opened: false })} />}
        <div className="trigger-wrapper">
          <div
            className={`trigger Select ${opened && "is-open" || ""}`}
          >
            <span className="Select-value-label">
              {selectedValue}
            </span>
            <div className="Select-control">
              <div className="Select-arrow-zone">
                <span className="Select-arrow"></span>
              </div>
            </div>
          </div>
          {opened && content}
        </div>
      </div>
    );
  }
}

export default RangeSelect;
