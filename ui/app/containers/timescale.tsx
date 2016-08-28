import * as React from "react";
import { connect } from "react-redux";
import classNames = require("classnames");
import _ = require("lodash");

import { AdminUIState } from "../redux/state";
import * as timewindow from "../redux/timewindow";

interface TimeScaleSelectorProps {
  currentScale: timewindow.TimeScale;
  availableScales: timewindow.TimeScaleCollection;
  setTimeScale: typeof timewindow.setTimeScale;
}

interface TimeScaleSelectorState {
  // This setting should not persist if the current route is changed, and thus
  // it is not stored in the redux state.
  controlsVisible: boolean;
}

class TimeScaleSelector extends React.Component<TimeScaleSelectorProps, TimeScaleSelectorState> {
  constructor() {
    super();
    this.state = {
      controlsVisible: false,
    };
  }

  toggleControls = () => {
    this.setState({
      controlsVisible: !this.state.controlsVisible,
    });
  };

  changeSettings(newSettings: timewindow.TimeScale) {
    this.props.setTimeScale(newSettings);
  }

  isSelected(scale: timewindow.TimeScale) {
    return scale === this.props.currentScale;
  }

  render() {
    let selectorClass = classNames({
      "timescale-selector": true,
      "show": this.state.controlsVisible,
    });
    return <div className="timescale-selector-container">
      <button className="timescale" onClick={this.toggleControls}>Select Timescale</button>
      <div className={selectorClass}>
        <div className="text">View Last: </div>
        {
          _.map(this.props.availableScales, (scale, key) => {
            let theseSettings = scale;
            return <button key={key}
                           className={classNames({selected: this.isSelected(scale)})}
                           onClick={() => this.changeSettings(theseSettings)}>
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
      currentScale: (state.timewindow as timewindow.TimeWindowState).scale,
      availableScales: timewindow.availableTimeScales,
    };
  },
  {
    setTimeScale : timewindow.setTimeScale,
  }
)(TimeScaleSelector);
