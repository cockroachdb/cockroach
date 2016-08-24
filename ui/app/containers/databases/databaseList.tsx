import * as React from "react";
import { Link, IInjectedProps } from "react-router";
import _ from "lodash";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import * as protos from "../../js/protos";
import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabases, refreshDatabaseDetails, KeyedCachedDataReducerState } from "../../redux/apiReducers";
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";

type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;

// Constant used to store sort settings in the redux UI store.
const UI_DATABASES_SORT_SETTING_KEY = "databaseList/sort_setting";

// DatabaseInfo is a structure that aggregates information from multiple backend
// queries regarding the same database.
class DatabaseInfo {
  constructor(public name: string, public numTables: number) { };
}

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const DatabasesSortedTable = SortedTable as new () => SortedTable<DatabaseInfo>;

/**
 * DatabaseListData are the data properties which should be passed to the DatabaseList
 * container.
 */
interface DatabaseListData {
  // Current sort setting for the table. Incoming rows will already be sorted
  // according to this setting.
  sortSetting: SortSetting;
  // A list of databases, which are possibly sorted according to
  // sortSetting.
  databaseInfos: DatabaseInfo[];
}

/**
 * DatabaseListActions are the action dispatchers which should be passed to the
 * DatabaseList container.
 */
interface DatabaseListActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
  refreshDatabases: typeof refreshDatabases;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
}

/**
 * DatabaseListProps is the type of the props object that must be passed to
 * DatabaseList component.
 */
type DatabaseListProps = DatabaseListData & DatabaseListActions & IInjectedProps;

/**
 * DatabaseList renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
class DatabaseList extends React.Component<DatabaseListProps, {}> {
  // Callback when the user elects to change the sort setting.
  changeSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASES_SORT_SETTING_KEY, setting);
  }

  // loadDatabaseDetails loads detailed data for each database with no info in 
  // the store.
  loadDatabaseDetails(props = this.props) {
    if (props.databaseInfos.length) {
      _.each(props.databaseInfos, (dbInfo) => {
        if (_.isUndefined(dbInfo.numTables)) {
          props.refreshDatabaseDetails(new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
            database: dbInfo.name,
          }));
        }
      });
    }
  }

  componentWillMount() {
    this.props.refreshDatabases();
    this.loadDatabaseDetails();
  }

  componentWillReceiveProps(props: DatabaseListProps) {
    this.loadDatabaseDetails(props);
  }

  render() {
    let { databaseInfos, sortSetting } = this.props;

    if (databaseInfos) {
      return <div className="sql-table">
        <DatabasesSortedTable
          data={databaseInfos}
          sortSetting={sortSetting}
          onChangeSortSetting={(setting) => this.changeSortSetting(setting)}
          columns={[
            {
              title: "Database Name",
              cell: (dbInfo) => <Link to={`databases/database/${dbInfo.name}`}>{dbInfo.name}</Link>,
              sort: (dbInfo) => dbInfo.name,
              className: "expand-link", // don't pad the td element to allow the link to expand
            },
            {
              title: "# Of Tables",
              cell: (dbInfo) => dbInfo.numTables,
              sort: (dbInfo) => dbInfo.numTables,
            },
            {
              title: "Last Modified",
              cell: (dbInfo) => "", // TODO (maxlang): Pending #8246
            },
            {
              title: "Events",
              cell: (dbInfo) => "", // TODO (maxlang): Pending #8246
            },
            {
              title: "Replication Factor",
              cell: (dbInfo) => "", // TODO (maxlang): Pending #8248
            },
            {
              title: "Target Range Size",
              cell: (dbInfo) => "", // TODO (maxlang): Pending #8248
            },
          ]} />
      </div>;
    }
    return <div>No results.</div>;
  }
}

// Base selectors to extract data from redux state.
let databases = (state: AdminUIState): string[] => state.cachedData.databases.data  && state.cachedData.databases.data.databases;
let databaseDetails = (state: AdminUIState) => state.cachedData.databaseDetails;
let sortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASES_SORT_SETTING_KEY] || {};

// Selector which generates the table rows as a DatabaseInfo[].
let databaseInfos = createSelector(
  databases,
  databaseDetails,
  (dbs: string[], details: KeyedCachedDataReducerState<DatabaseDetailsResponseMessage>): DatabaseInfo[] => {
    return _.map(dbs, (db) => new DatabaseInfo(db, details[db] && details[db].data && details[db].data.table_names.length));
  }
);

// Connect the DatabaseList class with our redux store.
let databaseListConnected = connect(
  (state: AdminUIState) => {
    return {
      databaseInfos: databaseInfos(state),
      sortSetting: sortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabases,
    refreshDatabaseDetails,
  }
)(DatabaseList);

export default databaseListConnected;
