/// <reference path="../../../typings/index.d.ts"/>

import _ from "lodash";
import * as React from "react";
import { connect } from "react-redux";
import { IInjectedProps } from "react-router";

import { ListLink } from "../../components/listLink";

import { AdminUIState } from "../../redux/state";
import { refreshDatabases } from "../../redux/apiReducers";

import DatabaseSummaryTables from "./databaseTables";
import DatabaseSummaryGrants from "./databaseGrants";

// excludedTableList is a list of virtual databases that should be excluded 
// from database lists; they are not physical databases, and thus cause issues
// with our backend methods.
// TODO(mrtracy): This exclusion should occur on the backend methods, which
// should handle virtual tables correctly. Github #9689.
const excludedTableList = {
  "information_schema": true,
  "pg_catalog": true,
};

// DatabaseListNav is a pure-function component that renders the navigation 
// element specific to the databases tab.
function DatabaseListNav(props: {children?: any}) {
  return <div className="nav-container">
    <ul className="nav">
    <ListLink to="/databases/tables">Tables</ListLink>
    <ListLink to="/databases/grants">Grants</ListLink>
      </ul>
  </div>;
}

// DatabaseListData describes properties which should be passed to the
// DatabaseList container.
interface DatabaseListData {
  // A list of databases.
  databaseNames: string[];
}

// DatabaseListActions describes actions that can be dispatched by a 
// DatabaseList component.
interface DatabaseListActions {
  refreshDatabases: typeof refreshDatabases;
}

type DatabaseListProps = DatabaseListData & DatabaseListActions & IInjectedProps;

// DatabaseTablesList displays the "Tables" sub-tab of the main database page.
class DatabaseTablesList extends React.Component<DatabaseListProps, {}> {
  static title() {
    return <h2>Database Tables</h2>;
  }

  componentWillMount() {
    this.props.refreshDatabases();
  }

  render() {
    return <div>
      <DatabaseListNav/>
      <div className="section databases">
        { _.map(this.props.databaseNames, (n) => {
          if (excludedTableList.hasOwnProperty(n)) {
            return null;
          }
          return <DatabaseSummaryTables name={n} key={n} />;
        }) }
      </div>
    </div>;
  }
}

// DatabaseTablesList displays the "Grants" sub-tab of the main database page.
class DatabaseGrantsList extends React.Component<DatabaseListProps, {}> {
  static title() {
    return <h2>Database Grants</h2>;
  }

  componentWillMount() {
    this.props.refreshDatabases();
  }

  render() {
    return <div>
      <DatabaseListNav/>
      <div className="section databases">
        { _.map(this.props.databaseNames, (n) => {
          if (excludedTableList.hasOwnProperty(n)) {
            return null;
          }
          return <DatabaseSummaryGrants name={n} key={n} />;
        }) }
      </div>
    </div>;
  }
}

// Base selectors to extract data from redux state.
let databaseNames = (state: AdminUIState): string[] => state.cachedData.databases.data && state.cachedData.databases.data.databases;

// Connect the DatabaseTablesList class with our redux store.
let databaseTablesListConnected = connect(
  (state: AdminUIState) => {
    return {
      databaseNames: databaseNames(state),
    };
  },
  {
    refreshDatabases,
  }
)(DatabaseTablesList);

// Connect the DatabaseGrantsList class with our redux store.
let databaseGrantsListConnected = connect(
  (state: AdminUIState) => {
    return {
      databaseNames: databaseNames(state),
    };
  },
  {
    refreshDatabases,
  }
)(DatabaseGrantsList);

export {
  databaseTablesListConnected as DatabaseTablesList,
  databaseGrantsListConnected as DatabaseGrantsList,
}
