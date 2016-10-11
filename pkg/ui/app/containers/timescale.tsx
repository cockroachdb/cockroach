import * as React from "react";
import { connect } from "react-redux";
import classNames from "classnames";
import _ from "lodash";
import moment from "moment";

import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import * as timewindow from "../redux/timewindow";
import { setUISetting } from "../redux/ui";

import { NodeStatus } from "../util/proto";
import { LongToMoment } from "../util/convert";

// Tracks whether the default timescale been set once in the app. Tracked across
// the entire app so that changing pages doesn't cause it to reset.
const UI_TIMESCALE_DEFAULT_SET = "timescale/default_set";

interface TimeScaleSelectorProps {
  currentScale: timewindow.TimeScale;
  availableScales: timewindow.TimeScaleCollection;
  setTimeScale: typeof timewindow.setTimeScale;
  // Track node data to find the oldest node and set the default timescale.
  refreshNodes: typeof refreshNodes;
  nodeStatuses: NodeStatus[];
  nodeStatusesValid: boolean;
  // Track whether the default has been set.
  setUISetting: typeof setUISetting;
  defaultTimescaleSet: boolean;
}

interface TimeScaleSelectorState {
  controlsVisible: boolean; // Timescale selector controls visible.
}

class TimeScaleSelector extends React.Component<TimeScaleSelectorProps, TimeScaleSelectorState> {

  timescaleBtn: Element;

  constructor() {
    super();
    this.state = {
      controlsVisible: false,
    };
  }

  setVisible = (visible: boolean) => {
    this.setState({
      controlsVisible: visible,
    });
  };

  hide = (e: Event) => {
    if (e.target !== this.timescaleBtn) {
      this.setVisible(false);
    }
  };

  changeSettings(newSettings: timewindow.TimeScale) {
    this.props.setTimeScale(newSettings);
  }

  isSelected(scale: timewindow.TimeScale) {
    return scale === this.props.currentScale;
  }

  // Sets the default timescale based on the start time of the oldest node.
  setDefaultTime(props = this.props) {
    if (props.nodeStatusesValid && !props.defaultTimescaleSet) {
      let oldestNode = _.minBy(props.nodeStatuses, (nodeStatus: NodeStatus) => nodeStatus.started_at);
      let clusterStarted = LongToMoment(oldestNode.started_at);
      let clusterDurationHrs = moment.utc().diff(clusterStarted, "hours");
      if (clusterDurationHrs > 1) {
        if (clusterDurationHrs < 6) {
          props.setTimeScale(props.availableScales["1 hour"]);
        } else if (clusterDurationHrs < 12) {
          props.setTimeScale(props.availableScales["6 hours"]);
        } else if (clusterDurationHrs < 24) {
          props.setTimeScale(props.availableScales["12 hours"]);
        } else {
          props.setTimeScale(props.availableScales["1 day"]);
        }
      }
      props.setUISetting(UI_TIMESCALE_DEFAULT_SET, true);
    }
  }

  componentWillMount() {
    // Hide the popup when you click anywhere on the page
    document.body.addEventListener("click", this.hide);
    this.props.refreshNodes();
    this.setDefaultTime();
  }

  componentWillReceiveProps(props: TimeScaleSelectorProps) {
    if (!props.nodeStatusesValid) {
      this.props.refreshNodes();
    } else {
      this.setDefaultTime(props);
    }
  }

  componentWillUnmount() {
    // Remove popup hiding event listener on unmount
    document.body.removeEventListener("click", this.hide);
  }

  render() {
    let selectorClass = classNames({
      "timescale-selector": true,
      "show": this.state.controlsVisible,
    });
    return <div className="timescale-selector-container">
      <button
        className="timescale"
        ref={(timescaleBtn) => this.timescaleBtn = timescaleBtn}
        onClick={() => this.setVisible(!this.state.controlsVisible)}>
          Select Timescale
      </button>
      <div className={selectorClass}>
        <div className="text">View Last: </div>
        {
          _.map(this.props.availableScales, (scale, key) => {
            let theseSettings = scale;
            return <button key={key}
                           className={classNames({selected: this.isSelected(scale)})}
                           onClick={(e) => { this.changeSettings(theseSettings); this.setVisible(false); } }>
                           { key }
            </button>;
          })
        }
      </div>
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
      defaultTimescaleSet: state.ui[UI_TIMESCALE_DEFAULT_SET] || false,
    };
  },
  {
    setTimeScale: timewindow.setTimeScale,
    refreshNodes: refreshNodes,
    setUISetting: setUISetting,
  }
)(TimeScaleSelector);
