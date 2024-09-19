// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  ColumnDescriptor,
  SortedTable,
  SortSetting,
  util,
  Timestamp,
} from "@cockroachlabs/cluster-ui";
import isNil from "lodash/isNil";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import * as protos from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";

import { BackToAdvanceDebug } from "../util";

import "./index.styl";

interface SettingsOwnProps {
  settings: CachedDataReducerState<protos.cockroach.server.serverpb.SettingsResponse>;
  refreshSettings: typeof refreshSettings;
}

interface IterableSetting {
  key: string;
  description?: string;
  type?: string;
  value?: string;
  public?: boolean;
  last_updated?: moment.Moment;
}

interface SettingsState {
  sortSetting: {
    ascending: boolean;
    columnTitle: string;
  };
}

type SettingsProps = SettingsOwnProps & RouteComponentProps;

/**
 * Renders the Cluster Settings Report page.
 */
export class Settings extends React.Component<SettingsProps, SettingsState> {
  constructor(props: SettingsProps) {
    super(props);
    this.state = {
      sortSetting: { ascending: true, columnTitle: "lastUpdated" },
    };
  }

  sortSetting: { ascending: boolean; columnTitle: string | null };

  refresh(props = this.props) {
    props.refreshSettings(
      new protos.cockroach.server.serverpb.SettingsRequest(),
    );
  }

  componentDidMount() {
    // Refresh settings query when mounting.
    this.refresh();
  }

  renderTable(wantPublic: boolean) {
    if (isNil(this.props.settings.data)) {
      return null;
    }

    const { key_values } = this.props.settings.data;
    const dataArray: IterableSetting[] = Object.keys(key_values)
      .map(key => ({
        key,
        ...key_values[key],
      }))
      .map(obj => {
        return {
          ...obj,
          last_updated: obj.last_updated
            ? util.TimestampToMoment(obj.last_updated)
            : null,
        };
      });
    const columns: ColumnDescriptor<IterableSetting>[] = [
      {
        name: "name",
        title: "Setting",
        cell: (setting: IterableSetting) => setting.key,
        sort: (setting: IterableSetting) => setting.key,
      },
      {
        name: "value",
        title: "Value",
        cell: (setting: IterableSetting) => setting.value,
      },
      {
        name: "lastUpdated",
        title: "Last Updated",
        cell: (setting: IterableSetting) => (
          <Timestamp
            time={setting.last_updated}
            format={util.DATE_FORMAT_24_TZ}
            fallback={"No overrides"}
          />
        ),
        sort: (setting: IterableSetting) => setting.last_updated?.valueOf(),
      },
      {
        name: "description",
        title: "Description",
        cell: (setting: IterableSetting) => setting.description,
      },
    ];

    return (
      <SortedTable
        data={dataArray.filter(obj =>
          wantPublic ? obj.public : obj.public === undefined,
        )}
        columns={columns}
        sortSetting={this.state.sortSetting}
        onChangeSortSetting={(ss: SortSetting) =>
          this.setState({
            sortSetting: {
              ascending: ss.ascending,
              columnTitle: ss.columnTitle,
            },
          })
        }
      />
    );
  }

  render() {
    return (
      <div className="section">
        <Helmet title="Cluster Settings | Debug" />
        <BackToAdvanceDebug history={this.props.history} />
        <h1 className="base-heading">Cluster Settings</h1>
        <Loading
          loading={!this.props.settings.data}
          page={"container settings"}
          error={this.props.settings.lastError}
          render={() => (
            <div>
              <p className="settings-note">
                Note that some settings have been redacted for security
                purposes.
              </p>
              {this.renderTable(true)}
              <h3>Reserved settings</h3>
              <p className="settings-note">
                Note that changes to the following settings can yield
                unpredictable or negative effects on the entire cluster. Use at
                your own risk.
              </p>
              {this.renderTable(false)}
            </div>
          )}
        />
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => ({
  // RootState contains declaration for whole state
  settings: state.cachedData.settings,
});

const mapDispatchToProps = {
  // actionCreators returns objects with type and payload
  refreshSettings,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(Settings),
);
