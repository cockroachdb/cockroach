// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Button, TimePicker, notification, Calendar, Icon } from "antd";
import moment, { Moment } from "moment";
import { TimeWindow } from "src/redux/timewindow";
import { trackTimeScaleSelected } from "src/util/analytics";
import React from "react";
import "./range.styl";
import { arrowRenderer } from "src/views/shared/components/dropdown";

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
  changeDate: (arg0: moment.Moment, arg1: DateTypes) => void;
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
    this.clearPanelValues();
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

  renderTimePickerAddon = (direction: DateTypes) => () => <Button type="default" onClick={() => this.onChangeDate(direction)(moment())} size="small">Now</Button>;

  renderDatePickerAddon = (direction: DateTypes) => <div className="calendar-today-btn"><Button type="default" onClick={() => this.onChangeDate(direction)(moment())} size="small">Today</Button></div>;

  clearPanelValues = () => this.setState({ selectMonthEnd: null, selectMonthStart: null });

  onChangeOption = (option: RangeOption) => () => {
    const { onChange } = this.props;
    this.toggleDropDown();
    onChange(option);
  }

  toggleCustomPicker = (custom: boolean) => () => this.setState({ custom }, this.clearPanelValues);

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
    });
  }

  handleOptionButtonOnClick = (option: RangeOption) => () => {
    trackTimeScaleSelected(option.label);
    (option.value === "Custom" ? this.toggleCustomPicker(true) : this.onChangeOption(option))();
  }

  optionButton = (option: RangeOption) => (
    <Button
      type="default"
      className={`_time-button ${this.props.selected.title === option.value && "active" || ""}`}
      onClick={this.handleOptionButtonOnClick(option)}
      ghost
    >
      <span className="range__range-title">{this.props.selected.title !== "Custom" && option.value === "Custom" ? "--" : option.timeLabel}</span>
      <span className="__option-label">{option.value === "Custom" ? "Custom date range" : option.value}</span>
    </Button>
  )

  renderOptions = () => {
    const { options } = this.props;
    return options.map(option => this.optionButton(option));
  }

  findSelectedValue = () => {
    const { options, selected } = this.props;
    const value = options.find(option => option.value === selected.title);
    return value && value.label !== "Custom" ? (
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

  headerRender = (item: any) => (
    <div className="calendar-month-picker">
      <Button type="default" onClick={() => item.onChange(moment(item.value).subtract(1, "months"))}><Icon type="left" /></Button>
      <span>{moment(item.value).format("MMM YYYY")}</span>
      <Button type="default" onClick={() => item.onChange(moment(item.value).add(1, "months"))}><Icon type="right" /></Button>
    </div>
  )

  onPanelChange = (direction: DateTypes) => (date: Moment) => {
    const { selectMonthStart, selectMonthEnd } = this.state;

    this.setState({
      selectMonthStart: direction === DateTypes.DATE_FROM ? date : selectMonthStart,
      selectMonthEnd: direction === DateTypes.DATE_TO ? date : selectMonthEnd,
    });
  }

  renderContent = () => {
    const { value, useTimeRange } = this.props;
    const { custom , selectMonthStart, selectMonthEnd } = this.state;
    const start = useTimeRange ? moment.utc(value.start) : null;
    const end = useTimeRange ? moment.utc(value.end) : null;
    const timePickerFormat = "h:mm:ss A";
    const isSameDate = useTimeRange && moment(start).isSame(end, "day");
    const calendarStartValue = selectMonthStart ? selectMonthStart : start ? start : moment();
    const calendarEndValue = selectMonthEnd ? selectMonthEnd : end ? end : moment();

    if (!custom) {
      return <div className="_quick-view">{this.renderOptions()}</div>;
    }

    return (
      <React.Fragment>
        <div className="_start">
          <span className="_title">From</span>
          <div className="range-calendar">
            <Calendar
              fullscreen={false}
              value={calendarStartValue}
              disabledDate={(currentDate) => (currentDate > (end || moment()))}
              headerRender={this.headerRender}
              onSelect={this.onChangeDate(DateTypes.DATE_FROM)}
              onPanelChange={this.onPanelChange(DateTypes.DATE_FROM)}
            />
            {this.renderDatePickerAddon(DateTypes.DATE_FROM)}
          </div>
          <TimePicker
            value={start}
            allowClear={false}
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
          <span className="_title">To</span>
          <div className="range-calendar">
            <Calendar
              fullscreen={false}
              value={calendarEndValue}
              disabledDate={(currentDate) => (currentDate > moment() || currentDate < (start || moment()))}
              headerRender={this.headerRender}
              onSelect={this.onChangeDate(DateTypes.DATE_TO)}
              onPanelChange={this.onPanelChange(DateTypes.DATE_TO)}
            />
            {this.renderDatePickerAddon(DateTypes.DATE_TO)}
          </div>
          <TimePicker
            value={end}
            allowClear={false}
            format={`${timePickerFormat} ${moment(end).isSame(moment.utc(), "minute") && "[- Now]" || ""}`}
            use12Hours
            addon={this.renderTimePickerAddon(DateTypes.DATE_TO)}
            onChange={this.onChangeDate(DateTypes.DATE_TO)}
            disabledHours={isSameDate && this.getDisabledHours() || undefined}
            disabledMinutes={isSameDate && this.getDisabledMinutes() || undefined}
            disabledSeconds={isSameDate && this.getDisabledSeconds() || undefined}
          />
        </div>
      </React.Fragment>
    );
  }

  render() {
    const { opened, width, custom } = this.state;
    const selectedValue = this.findSelectedValue();
    const containerLeft = this.rangeContainer.current ? this.rangeContainer.current.getBoundingClientRect().left : 0;
    const left = width >= (containerLeft + (custom ? 555 : 453)) ? 0 : width - (containerLeft + (custom ? 555 : 453));

    return (
      <div ref={this.rangeContainer} className="Range">
        <div className="click-zone" onClick={this.toggleDropDown}/>
        {opened && <div className="trigger-container" onClick={this.toggleDropDown} />}
        <div className="trigger-wrapper">
          <div
            className={`trigger Select ${opened ? "is-open" : ""}`}
          >
            <span className="Select-value-label">
              {selectedValue}
            </span>
            <div className="Select-control">
              <div className="Select-arrow-zone">
                {arrowRenderer({ isOpen: opened })}
              </div>
            </div>
          </div>
          {opened && (
            <div className={`range-selector ${custom ? "__custom" : "__options"}`} style={{ left }}>
              {this.renderContent()}
            </div>
          )}
        </div>
      </div>
    );
  }
}

export default RangeSelect;
