import * as React from "react";
import { connect } from "react-redux";
import classNames from "classnames";
import _ from "lodash";

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

  componentWillMount() {
    // Hide the popup when you click anywhere on the page
    document.body.addEventListener("click", this.hide);
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
      currentScale: (state.timewindow as timewindow.TimeWindowState).scale,
      availableScales: timewindow.availableTimeScales,
    };
  },
  {
    setTimeScale : timewindow.setTimeScale,
  }
)(TimeScaleSelector);
