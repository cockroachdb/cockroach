import _ from "lodash";
import * as React from "react";
import { connect } from "react-redux";
import Banner from "./banner";

import { AdminUIState } from "../../redux/state";
import { refreshWarnings } from "../../redux/apiReducers";
import { CachedDataReducerState } from "../../redux/cachedDataReducer";
import * as api from "../../util/api";

class WarningsBannerProps {
  warnings: CachedDataReducerState<api.WarningsResponseMessage>;
  refreshWarnings: typeof refreshWarnings;
}

class WarningsBannerState {
  dismissed: boolean = false;
}

class WarningsBanner extends React.Component<WarningsBannerProps, WarningsBannerState> {
  state = new WarningsBannerState();

  componentWillMount() {
    this.props.refreshWarnings();
  }

  render() {
    let visible: boolean = this.props.warnings && this.props.warnings.data && this.props.warnings.data.warnings.length > 0 && !this.state.dismissed;
    return <Banner className="disconnected" visible={visible} onclose={() => this.setState({dismissed: true}) }>
      <span className="icon-warning" />
      {
        (this.props.warnings && this.props.warnings.data) ?  _.map(this.props.warnings.data.warnings, (warning) => {
          return <span key={warning.server}>{warning.server}: {warning.message}<br/></span>;
        }) : ""
      }
    </Banner>;
  }
}

// Connect the WarningsBanner class with our redux store.
let disconnectedBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      warnings: state.cachedData.warnings,
    };
  },
  {
    refreshWarnings,
  }
)(WarningsBanner);

export default disconnectedBannerConnected;
