// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import Loading from "src/views/shared/components/loading";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { AdminUIState } from "src/redux/state";
import { refreshAlerts } from "src/redux/apiReducers";
import { cockroach } from "src/js/protos";
import { AlertsResponseMessage } from "src/util/api";
import "./alertsPage.styl";

import IHealthAlert = cockroach.server.status.statuspb.IHealthAlert;
import IHealthCheckResult = cockroach.server.status.statuspb.IHealthCheckResult;
import HealthAlert = cockroach.server.status.statuspb.HealthAlert;

const listOfAlerts = createSelector(
  (alertsResponse: AlertsResponseMessage) => alertsResponse.alerts,
  (alerts: { [nodeID: string]: IHealthCheckResult }): NodeAlert[] => {
    const output: NodeAlert[] = [];
    _.map(alerts, (nodeAlerts: IHealthCheckResult, nodeID) => {
      nodeAlerts.alerts.forEach((alert) => {
        output.push({
          alert,
          nodeID,
        });
      });
    });
    return output;
  },
);

class AlertsSortedTable extends SortedTable<NodeAlert> {}

const COLUMNS: ColumnDescriptor<NodeAlert>[] = [
  {
    title: "Node ID",
    cell: (alert) => alert.nodeID,
    sort: (alert) => alert.nodeID,
  },
  {
    title: "Store ID",
    cell: (alert) => alert.alert.store_id,
    sort: (alert) => alert.alert.store_id,
  },
  {
    title: "Category",
    cell: (alert) => HealthAlert.Category[alert.alert.category],
    sort: (alert) => alert.alert.category,
  },
  {
    title: "Description",
    cell: (alert) => <code>{alert.alert.description}</code>,
    sort: (alert) => alert.alert.description,
  },
  {
    title: "Value",
    cell: (alert) => alert.alert.value,
    sort: (alert) => alert.alert.value,
  },
];

interface NodeAlert {
  nodeID: string;
  alert: IHealthAlert;
}

interface AlertsPageProps {
  alerts: CachedDataReducerState<AlertsResponseMessage>;
  refreshAlerts: typeof refreshAlerts;
}

interface AlertsPageState {
  sortSetting: SortSetting;
}

class AlertsPage extends React.Component<AlertsPageProps, AlertsPageState> {

  constructor(props: AlertsPageProps) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 0,
        ascending: false,
      },
    };
  }

  componentDidMount() {
    this.props.refreshAlerts();
  }

  changeSortSetting = (sortSetting: SortSetting) => {
    this.setState({
      sortSetting,
    });
  }

  renderAlertsTable = () => {
    const alerts = listOfAlerts(this.props.alerts.data);

    return (
      <AlertsSortedTable
        className="alerts-table"
        data={alerts}
        columns={COLUMNS}
        sortSetting={this.state.sortSetting}
        onChangeSortSetting={this.changeSortSetting}
      />
    );
  }

  render() {
    return (
      <React.Fragment>
        <Helmet>
          <title>Alerts</title>
        </Helmet>

        <section className="section">
          <h1>Alerts</h1>
        </section>

        <section className="section">
          <Loading
            loading={!this.props.alerts.data}
            error={this.props.alerts.lastError}
            render={this.renderAlertsTable}
          />
        </section>
      </React.Fragment>
    );
  }

}

// tslint:disable-next-line:variable-name
const AlertsPageConnected = connect(
  (state: AdminUIState) => ({
    alerts: state.cachedData.alerts,
  }),
  {
    refreshAlerts,
  },
)(AlertsPage);

export default AlertsPageConnected;
