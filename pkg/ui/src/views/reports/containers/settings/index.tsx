import _ from "lodash";
import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import Loading from "src/views/shared/components/loading";

import spinner from "assets/spinner.gif";

interface SettingsOwnProps {
  settings: CachedDataReducerState<protos.cockroach.server.serverpb.SettingsResponse>;
  refreshSettings: typeof refreshSettings;
}

type SettingsProps = SettingsOwnProps;

/**
 * Renders the Cluster Settings Report page.
 */
class Settings extends React.Component<SettingsProps, {}> {
  refresh(props = this.props) {
    props.refreshSettings(new protos.cockroach.server.serverpb.SettingsRequest());
  }

  componentWillMount() {
    // Refresh settings query when mounting.
    this.refresh();
  }

  renderTable() {
    if (_.isNil(this.props.settings.data)) {
      return null;
    }

    const { key_values } = this.props.settings.data;

    return (
      <table className="settings-table">
        <thead>
          <tr className="settings-table__row settings-table__row--header">
            <th className="settings-table__cell settings-table__cell--header">Setting</th>
            <th className="settings-table__cell settings-table__cell--header">Value</th>
            <th className="settings-table__cell settings-table__cell--header">Description</th>
          </tr>
        </thead>
        <tbody>
          {
            _.chain(_.keys(key_values))
              .sort()
              .map(key => (
                <tr key={key} className="settings-table__row">
                  <td className="settings-table__cell">{key}</td>
                  <td className="settings-table__cell">{key_values[key].value}</td>
                  <td className="settings-table__cell">{key_values[key].description}</td>
                </tr>
              ))
              .value()
          }
        </tbody>
      </table>
    );
  }

  render() {
    if (!_.isNil(this.props.settings.lastError)) {
      return (
        <div className="section">
          <h1>Cluster Settings</h1>
          <h2>Error loading Cluster Settings</h2>
          {this.props.settings.lastError}
        </div>
      );
    }

    return (
      <div className="section">
        <h1>Cluster Settings</h1>
        <h2></h2>
        <Loading
          loading={!this.props.settings.data}
          className="loading-image loading-image__spinner-left loading-image__spinner-left__padded"
          image={spinner}
        >
          <div>
            <p>Note that some settings have been redacted for security purposes.</p>
            {this.renderTable()}
          </div>
        </Loading>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    settings: state.cachedData.settings,
  };
}

const actions = {
  refreshSettings,
};

export default connect(mapStateToProps, actions)(Settings);
