import * as React from "react";
import { connect } from "react-redux";
import _ from "lodash";
import moment from "moment";

import Dropdown, { DropdownOption, ArrowDirection } from "../components/dropdown";

import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import * as timewindow from "../redux/timewindow";
import { LocalSetting } from "../redux/localsettings";

import { NodeStatus$Properties } from "../util/proto";
import { LongToMoment } from "../util/convert";

// Tracks whether the default timescale been set once in the app. Tracked across
// the entire app so that changing pages doesn't cause it to reset.
const timescaleDefaultSet = new LocalSetting(
  "timescale/default_set", (s: AdminUIState) => s.localSettings, false,
);

interface TimeRangeProps {
  start: moment.Moment;
  end: moment.Moment;
}

// TimeRange is a prettified string representation of the current timescale.
class TimeRange extends React.Component<TimeRangeProps, {}> {
  render() {
    let s = this.props.start.clone().utc();
    let e = this.props.end.clone().utc();
    let startTimeSpan = <span className="time-range__time">{s.format("HH:mm:ss")}</span>;
    let endTimeSpan = <span className="time-range__time">{e.format("HH:mm:ss")}</span>;
    let startDateSpan = <span className="time-range__date">{s.format("MMM DD, YYYY")}</span>;
    let endDateSpan = <span className="time-range__date">{e.format("MMM DD, YYYY")}</span>;

    let rangeHours = e.diff(s, "hours");
    let pastHours = moment().diff(s, "hours");
    // If start and end times are on the same day, show the times.
    if (rangeHours < 24 && s.date() === e.date()) {
      // If start and end times are today, omit the date, otherwise include it.
      if (pastHours < 24 && s.date() === moment().date()) {
        return <span className="time-range">{startTimeSpan} to {endTimeSpan}</span>;
      } else {
        return <span className="time-range">{startTimeSpan} to {endTimeSpan} on {endDateSpan}</span>;
      }
    }
    // If start and end times are within 48 hours show times and dates,
    // otherwise omit times and only show the dates.
    if ( rangeHours < 48) {
      return <span className="time-range">{startTimeSpan} on {startDateSpan} to {endTimeSpan} on {endDateSpan}</span>;
    } else {
      return <span className="time-range">{startDateSpan} to {endDateSpan}</span>;
    }
  }
}

interface TimeScaleDropdownProps {
  currentScale: timewindow.TimeScale;
  availableScales: timewindow.TimeScaleCollection;
  setTimeScale: typeof timewindow.setTimeScale;
  // Track node data to find the oldest node and set the default timescale.
  refreshNodes: typeof refreshNodes;
  nodeStatuses: NodeStatus$Properties[];
  nodeStatusesValid: boolean;
  // Track whether the default has been set.
  setDefaultSet: typeof timescaleDefaultSet.set;
  defaultTimescaleSet: boolean;
}

// TimeScaleDropdown is the dropdown that allows users to select the time range
// for graphs.
class TimeScaleDropdown extends React.Component<TimeScaleDropdownProps, {}> {
  changeSettings = (newTimescaleKey: DropdownOption) => {
    let newSettings = timewindow.availableTimeScales[newTimescaleKey.value];
    if (newSettings) {
      this.props.setTimeScale(newSettings);
    }
  }

  arrowClick = (direction: ArrowDirection) => {
    let selected = _.clone(this.props.currentScale);

    switch (direction) {
      case ArrowDirection.RIGHT:
        if (selected.windowEnd) {
          selected.windowEnd.add(selected.windowSize);
        }
        break;
      case ArrowDirection.LEFT:
        selected.windowEnd = selected.windowEnd || moment();
        selected.windowEnd.subtract(selected.windowSize);
        break;
      default:
        console.error("Unknown direction: ", direction);
    }

    // If the timescale extends into the future then fallback to a default
    // timescale. Otherwise set the key to "Custom" so it appears correctly.
    if (!selected.windowEnd || selected.windowEnd > moment().subtract(selected.windowValid)) {
      selected = _.find(timewindow.availableTimeScales, { windowSize: selected.windowSize });
    } else {
      selected.key = "Custom";
    }

    this.props.setTimeScale(selected);
  }

  getTimescaleOptions = () => {
    let timescaleOptions = _.map(timewindow.availableTimeScales, (_ts, k) => {
      return { value: k, label: "Last " + k };
    });

    // This just ensures that if the key is "Custom" it will show up in the
    // dropdown options. If a custom value isn't currently selected, "Custom"
    // won't show up in the list of options.
    if (!_.has(timewindow.availableTimeScales, this.props.currentScale.key)) {
      timescaleOptions.push({
        value: this.props.currentScale.key,
        label: this.props.currentScale.key,
      });
    }
    return timescaleOptions;
  }

  // Sets the default timescale based on the start time of the oldest node.
  setDefaultTime(props = this.props) {
    if (props.nodeStatusesValid && !props.defaultTimescaleSet) {
      let oldestNode = _.minBy(props.nodeStatuses, (nodeStatus: NodeStatus$Properties) => nodeStatus.started_at);
      let clusterStarted = LongToMoment(oldestNode.started_at);
      // TODO (maxlang): This uses the longest uptime, not the oldest
      let clusterDurationHrs = moment.utc().diff(clusterStarted, "hours");
      if (clusterDurationHrs > 1) {
        if (clusterDurationHrs < 6) {
          props.setTimeScale(props.availableScales["1 hour"]);
        } else if (clusterDurationHrs < 12) {
          props.setTimeScale(props.availableScales["6 hours"]);
        }
      }
      props.setDefaultSet(true);
    }
  }

  componentWillMount() {
    this.props.refreshNodes();
    this.setDefaultTime();
  }

  componentWillReceiveProps(props: TimeScaleDropdownProps) {
    if (!props.nodeStatusesValid) {
      this.props.refreshNodes();
    } else {
      this.setDefaultTime(props);
    }
  }

  render() {
    let currentScale = this.props.currentScale;
    let timeRange: React.ReactNode = "";
    let disabledArrows = [];

    // If the time scale doesn't end at the present, display the time range.
    if (currentScale.windowEnd) {
      let start = currentScale.windowEnd.clone().subtract(currentScale.windowSize);
      timeRange = <TimeRange start={start} end={currentScale.windowEnd} />;
    } else {
      // If the time scale does end at the present hide the right arrow.
      disabledArrows.push(ArrowDirection.RIGHT);
    }

    return <div>
      <Dropdown
        title=""
        options={this.getTimescaleOptions()}
        selected={currentScale.key}
        onChange={this.changeSettings}
        onArrowClick={this.arrowClick}
        disabledArrows={disabledArrows}
        />
      <span>{timeRange}</span>
    </div>;
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      nodeStatusesValid: state.cachedData.nodes.valid,
      nodeStatuses: state.cachedData.nodes.data,
      currentScale: (state.timewindow as timewindow.TimeWindowState).scale,
      availableScales: timewindow.availableTimeScales,
      defaultTimescaleSet: timescaleDefaultSet.selector(state),
    };
  },
  {
    setTimeScale: timewindow.setTimeScale,
    refreshNodes: refreshNodes,
    setDefaultSet: timescaleDefaultSet.set,
  },
)(TimeScaleDropdown);
