import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { SettingsResponseMessage } from "src/util/api";
import buildLoading from "src/views/shared/components/loading2";

// tslint:disable-next-line:variable-name
const Loading = buildLoading<SettingsResponseMessage>();

interface SettingsOwnProps {
  settings: CachedDataReducerState<SettingsResponseMessage>;
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

  renderContent(settings: SettingsResponseMessage) {
    const { key_values } = settings;

    return (
      <div>
        <p>Note that some settings have been redacted for security purposes.</p>
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
      </div>
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
        <Helmet>
          <title>Cluster Settings | Debug</title>
        </Helmet>
        <h1>Cluster Settings</h1>
        <h2></h2>
        <Loading
          data={this.props.settings}
          className="loading-image loading-image__spinner-left loading-image__spinner-left__padded"
        >
          { this.renderContent }
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
