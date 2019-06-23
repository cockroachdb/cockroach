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
import PropTypes from "prop-types";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { InjectedRouter, RouterState } from "react-router";
import { createSelector } from "reselect";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";

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
const systemDatabases = [
  "defaultdb",
  "postgres",
  "system",
];

// DatabaseListNav displays the database page navigation bar.
class DatabaseListNav extends React.Component<{selected: string}, {}> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  // TODO(mrtracy): Switch this, and the other uses of contextTypes, to use the
  // 'withRouter' HoC after upgrading to react-router 4.x.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState; };

  render() {
    return <PageConfig>
      <PageConfigItem>
        <Dropdown title="View" options={databasePages} selected={this.props.selected}
                  onChange={(selected: DropdownOption) => {
                    this.context.router.push(`databases/${selected.value}`);
                  }} />
      </PageConfigItem>
    </PageConfig>;
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

type DatabaseListProps = DatabaseListData & DatabaseListActions;

// DatabaseTablesList displays the "Tables" sub-tab of the main database page.
class DatabaseTablesList extends React.Component<DatabaseListProps, {}> {
  componentWillMount() {
    this.props.refreshDatabases();
  }

  render() {
    const { user, system } = this.props.databasesByType;

    return <div>
      <Helmet>
        <title>Tables | Databases</title>
      </Helmet>
      <section className="section"><h1>Databases</h1></section>
      <DatabaseListNav selected="tables"/>
      <div className="section databases">
        {
          user.map(n => <DatabaseSummaryTables name={n} key={n} />)
        }
        <hr />
        {
          system.map(n => <DatabaseSummaryTables name={n} key={n} />)
        }
        <NonTableSummary />
      </div>
    </div>;
  }
}

// DatabaseTablesList displays the "Grants" sub-tab of the main database page.
class DatabaseGrantsList extends React.Component<DatabaseListProps, {}> {
  componentWillMount() {
    this.props.refreshDatabases();
  }

  render() {
    const { user, system } = this.props.databasesByType;

    return <div>
      <Helmet>
        <title>Grants | Databases</title>
      </Helmet>
      <section className="section"><h1>Databases</h1></section>
      <DatabaseListNav selected="grants"/>
      <div className="section databases">
        {
          user.map(n => <DatabaseSummaryGrants name={n} key={n} />)
        }
        <hr />
        {
          system.map(n => <DatabaseSummaryGrants name={n} key={n} />)
        }
      </div>
    </div>;
  }
}

type DatabasesState = Pick<AdminUIState, "cachedData", "databases">;

// Base selectors to extract data from redux state.
function databaseNames(state: DatabasesState): string[] {
  if (state.cachedData.databases.data && state.cachedData.databases.data.databases) {
    return state.cachedData.databases.data.databases;
  }
  return [];
}

export const selectDatabasesByType = createSelector(
  databaseNames,
  (dbs: string[]) => {
    const [user, system] = _.partition(dbs, (db) => systemDatabases.indexOf(db) === -1);
    return { user, system };
  },
);

// Connect the DatabaseTablesList class with our redux store.
const databaseTablesListConnected = connect(
  (state: AdminUIState) => {
    return {
      databasesByType: selectDatabasesByType(state),
    };
  },
  {
    refreshDatabases,
  },
)(DatabaseTablesList);

// Connect the DatabaseGrantsList class with our redux store.
const databaseGrantsListConnected = connect(
  (state: AdminUIState) => {
    return {
      databasesByType: selectDatabasesByType(state),
    };
  },
  {
    refreshDatabases,
  },
)(DatabaseGrantsList);

export {
  databaseTablesListConnected as DatabaseTablesList,
  databaseGrantsListConnected as DatabaseGrantsList,
};
