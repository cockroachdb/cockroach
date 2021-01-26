// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { withRouter, RouteComponentProps } from "react-router-dom";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";

import { AdminUIState } from "src/redux/state";
import { refreshDatabases } from "src/redux/apiReducers";
import { Pick } from "src/util/pick";

import DatabaseSummaryTables from "src/views/databases/containers/databaseTables";
import DatabaseSummaryGrants from "src/views/databases/containers/databaseGrants";
import NonTableSummary from "./nonTableSummary";

import "./databases.styl";

const databasePages = [
  { value: "tables", label: "Tables" },
  { value: "grants", label: "Grants" },
];

// The system databases should sort after user databases.
const systemDatabases = ["defaultdb", "postgres", "system"];

interface DatabaseListNavProps {
  selected: string;
  onChange: (value: string) => void;
}
// DatabaseListNav displays the database page navigation bar.
class DatabaseListNav extends React.Component<DatabaseListNavProps> {
  render() {
    const { selected, onChange } = this.props;
    return (
      <PageConfig>
        <PageConfigItem>
          <Dropdown
            title="View"
            options={databasePages}
            selected={selected}
            onChange={({ value }: DropdownOption) => {
              onChange(value);
            }}
          />
        </PageConfigItem>
      </PageConfig>
    );
  }
}

// DatabaseListData describes properties which should be passed to the
// DatabaseList container.
interface DatabaseListData {
  // A list of databases for the user and the system.
  databasesByType: {
    user: string[];
    system: string[];
  };
}

// DatabaseListActions describes actions that can be dispatched by a
// DatabaseList component.
interface DatabaseListActions {
  refreshDatabases: typeof refreshDatabases;
}

type DatabaseListProps = DatabaseListData &
  DatabaseListActions &
  RouteComponentProps;

// DatabaseTablesList displays the "Tables" sub-tab of the main database page.
class DatabaseTablesList extends React.Component<DatabaseListProps> {
  componentDidMount() {
    this.props.refreshDatabases();
  }

  handleOnNavigationListChange = (value: string) => {
    this.props.history.push(`/databases/${value}`);
  };

  render() {
    const { user, system } = this.props.databasesByType;

    return (
      <div>
        <Helmet title="Tables | Databases" />
        <section className="section">
          <h1 className="base-heading">Databases</h1>
        </section>
        <DatabaseListNav
          selected="tables"
          onChange={this.handleOnNavigationListChange}
        />
        <div className="section databases">
          {user.map((n) => (
            <DatabaseSummaryTables name={n} key={n} updateOnLoad={false} />
          ))}
          {system.map((n) => (
            <DatabaseSummaryTables name={n} key={n} updateOnLoad={false} />
          ))}
          <NonTableSummary />
        </div>
      </div>
    );
  }
}

// DatabaseTablesList displays the "Grants" sub-tab of the main database page.
class DatabaseGrantsList extends React.Component<DatabaseListProps> {
  componentDidMount() {
    this.props.refreshDatabases();
  }

  handleOnNavigationListChange = (value: string) => {
    this.props.history.push(`/databases/${value}`);
  };

  render() {
    const { user, system } = this.props.databasesByType;

    return (
      <div>
        <Helmet title="Grants | Databases" />
        <section className="section">
          <h1 className="base-heading">Databases</h1>
        </section>
        <DatabaseListNav
          selected="grants"
          onChange={this.handleOnNavigationListChange}
        />
        <div className="section databases">
          {user.map((n) => (
            <DatabaseSummaryGrants name={n} key={n} />
          ))}
          {system.map((n) => (
            <DatabaseSummaryGrants name={n} key={n} />
          ))}
        </div>
      </div>
    );
  }
}

type DatabasesState = Pick<AdminUIState, "cachedData", "databases">;

// Base selectors to extract data from redux state.
function databaseNames(state: DatabasesState): string[] {
  if (
    state.cachedData.databases.data &&
    state.cachedData.databases.data.databases
  ) {
    return state.cachedData.databases.data.databases;
  }
  return [];
}

export const selectDatabasesByType = createSelector(
  databaseNames,
  (dbs: string[]) => {
    const [user, system] = _.partition(
      dbs,
      (db) => systemDatabases.indexOf(db) === -1,
    );
    return { user, system };
  },
);

const mapStateToProps = (state: AdminUIState) => ({
  // RootState contains declaration for whole state
  databasesByType: selectDatabasesByType(state),
});

const mapDispatchToProps = {
  refreshDatabases,
};

// Connect the DatabaseTablesList class with our redux store.
const databaseTablesListConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(DatabaseTablesList),
);

// Connect the DatabaseGrantsList class with our redux store.
const databaseGrantsListConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(DatabaseGrantsList),
);

export {
  databaseTablesListConnected as DatabaseTablesList,
  databaseGrantsListConnected as DatabaseGrantsList,
};
