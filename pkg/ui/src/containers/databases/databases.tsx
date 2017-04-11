import _ from "lodash";
import * as React from "react";
import { connect } from "react-redux";
import { InjectedRouter, RouterState } from "react-router";

import Dropdown, { DropdownOption } from "../../components/dropdown";
import { PageConfig, PageConfigItem } from "../../components/pageconfig";

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

let databasePages = [
  { value: "tables", label: "Tables" },
  { value: "grants", label: "Grants" },
];

// DatabaseListNav displays the database page navigation bar.
class DatabaseListNav extends React.Component<{selected: string}, {}> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  static contextTypes = {
    router: React.PropTypes.object.isRequired,
  };
  context: { router?: InjectedRouter & RouterState; };

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
  // A list of databases.
  databaseNames: string[];
}

// DatabaseListActions describes actions that can be dispatched by a
// DatabaseList component.
interface DatabaseListActions {
  refreshDatabases: typeof refreshDatabases;
}

type DatabaseListProps = DatabaseListData & DatabaseListActions;

// DatabaseTablesList displays the "Tables" sub-tab of the main database page.
class DatabaseTablesList extends React.Component<DatabaseListProps, {}> {
  static title() {
    return "Databases";
  }

  componentWillMount() {
    this.props.refreshDatabases();
  }

  render() {
    return <div>
      <DatabaseListNav selected="tables"/>
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
    return "Databases";
  }

  componentWillMount() {
    this.props.refreshDatabases();
  }

  render() {
    return <div>
      <DatabaseListNav selected="grants"/>
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
  },
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
  },
)(DatabaseGrantsList);

export {
  databaseTablesListConnected as DatabaseTablesList,
  databaseGrantsListConnected as DatabaseGrantsList,
};
