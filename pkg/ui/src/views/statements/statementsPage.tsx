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
import { RouteComponentProps } from "react-router";
import { createSelector } from "reselect";

import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { PrintTime } from "src/views/reports/containers/range/print";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { refreshStatements } from "src/redux/apiReducers";
import { StatementsResponseMessage } from "src/util/api";
import { aggregateStatementStats, flattenStatementStats, combineStatementStats, StatementStatistics, ExecutionStatistics } from "src/util/appStats";
import { appAttr } from "src/util/constants";
import { TimestampToMoment } from "src/util/convert";
import { Pick } from "src/util/pick";

import { AggregateStatistics, StatementsSortedTable, makeStatementsColumns } from "./statementsTable";

import * as protos from "src/js/protos";
import "./statements.styl";

type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type RouteProps = RouteComponentProps<any, any>;

interface StatementsPageProps {
  statements: AggregateStatistics[];
  statementsError: Error | null;
  apps: string[];
  totalFingerprints: number;
  lastReset: string;
  refreshStatements: typeof refreshStatements;
}

interface StatementsPageState {
  sortSetting: SortSetting;
}

class StatementsPage extends React.Component<StatementsPageProps & RouteProps, StatementsPageState> {

  constructor(props: StatementsPageProps & RouteProps) {
    super(props);
    this.state = {
      sortSetting: {
        sortKey: 1,
        ascending: false,
      },
    };
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  }

  selectApp = (app: DropdownOption) => {
    this.props.router.push(`/statements/${app.value}`);
  }

  componentWillMount() {
    this.props.refreshStatements();
  }

  componentWillReceiveProps() {
    this.props.refreshStatements();
  }

  renderStatements = () => {
    const selectedApp = this.props.params[appAttr] || "";
    const appOptions = [{ value: "", label: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, label: app }));

    const lastClearedHelpText = (
      <React.Fragment>
        Statement history is cleared once an hour by default, which can be
        configured with the cluster setting{" "}
        <code><pre style={{ display: "inline-block" }}>diagnostics.reporting.interval</pre></code>.
      </React.Fragment>
    );

    return (
      <React.Fragment>
        <PageConfig layout="spread">
          <PageConfigItem>
            <Dropdown
              title="App"
              options={appOptions}
              selected={selectedApp}
              onChange={this.selectApp}
            />
          </PageConfigItem>
          <PageConfigItem>
            <h4 className="statement-count-title">
              {this.props.statements.length}
              {selectedApp ? ` of ${this.props.totalFingerprints} ` : " "}
              statement fingerprints.
            </h4>
          </PageConfigItem>
          <PageConfigItem>
            <h4 className="last-cleared-title">
              <div className="last-cleared-tooltip__tooltip">
                <ToolTipWrapper text={lastClearedHelpText}>
                  <div className="last-cleared-tooltip__tooltip-hover-area">
                    <div className="last-cleared-tooltip__info-icon">i</div>
                  </div>
                </ToolTipWrapper>
              </div>
              Last cleared {this.props.lastReset}.
            </h4>
          </PageConfigItem>
        </PageConfig>

        <section className="section">
          <StatementsSortedTable
            className="statements-table"
            data={this.props.statements}
            columns={makeStatementsColumns(this.props.statements, selectedApp)}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
          />
        </section>
      </React.Fragment>
    );
  }

  render() {
    return (
      <React.Fragment>
        <Helmet>
          <title>
            { this.props.params[appAttr] ? this.props.params[appAttr] + " App | Statements" : "Statements"}
          </title>
        </Helmet>

        <section className="section">
          <h1>Statements</h1>
        </section>

        <Loading
          loading={_.isNil(this.props.statements)}
          error={this.props.statementsError}
          render={this.renderStatements}
        />
      </React.Fragment>
    );
  }
}

type StatementsState = Pick<AdminUIState, "cachedData", "statements">;

// selectStatements returns the array of AggregateStatistics to show on the
// StatementsPage, based on if the appAttr route parameter is set.
export const selectStatements = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (_state: StatementsState, props: { params: { [key: string]: string } }) => props,
  (state: CachedDataReducerState<StatementsResponseMessage>, props: RouteProps) => {
    if (!state.data) {
      return null;
    }

    let statements = flattenStatementStats(state.data.statements);
    if (props.params[appAttr]) {
      let criteria = props.params[appAttr];
      if (criteria === "(unset)") {
        criteria = "";
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) => statement.app === criteria,
      );
    }

    const statementsMap: { [statement: string]: StatementStatistics[] } = {};
    statements.forEach(stmt => {
      const matches = statementsMap[stmt.statement] || (statementsMap[stmt.statement] = []);
      matches.push(stmt.stats);
    });

    return Object.keys(statementsMap).map(stmt => {
      const stats = statementsMap[stmt];
      return {
        label: stmt,
        stats: combineStatementStats(stats),
      };
    });
  },
);

// selectApps returns the array of all apps with statement statistics present
// in the data.
export const selectApps = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return [];
    }

    let sawBlank = false;
    const apps: { [app: string]: boolean } = {};
    state.data.statements.forEach(
      (statement: ICollectedStatementStatistics) => {
        if (statement.key.key_data.app) {
          apps[statement.key.key_data.app] = true;
        } else {
          sawBlank = true;
        }
      },
    );
    return (sawBlank ? ["(unset)"] : []).concat(Object.keys(apps));
  },
);

// selectTotalFingerprints returns the count of distinct statement fingerprints
// present in the data.
export const selectTotalFingerprints = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return 0;
    }
    const aggregated = aggregateStatementStats(state.data.statements);
    return aggregated.length;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: StatementsState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return "unknown";
    }

    return PrintTime(TimestampToMoment(state.data.last_reset));
  },
);

// tslint:disable-next-line:variable-name
const StatementsPageConnected = connect(
  (state: StatementsState, props: RouteProps) => ({
    statements: selectStatements(state, props),
    statementsError: state.cachedData.statements.lastError,
    apps: selectApps(state),
    totalFingerprints: selectTotalFingerprints(state),
    lastReset: selectLastReset(state),
  }),
  {
    refreshStatements,
  },
)(StatementsPage);

export default StatementsPageConnected;
