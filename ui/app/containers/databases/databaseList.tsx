import * as React from "react";
import { Link, IInjectedProps } from "react-router";
import _ from "lodash";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import * as protos from "../../js/protos";
import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabases, refreshDatabaseDetails, refreshTableDetails, refreshEvents, KeyedCachedDataReducerState } from "../../redux/apiReducers";
import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;

// Constant used to store sort settings in the redux UI store.
const UI_DATABASES_SORT_SETTING_KEY = "databaseList/sort_setting";

class DatabaseInfo {
  constructor(public name: string, public numTables: number) { };
}

/******************************
 *      COLUMN DEFINITION
 */

/**
 * DatabasesTableColumn provides an enumeration value for each column in the databases table.
 */
enum DatabasesTableColumn {
  Name = 1,
  NumTables = 2,
  LastModified = 3,
  EventsLink = 4,
  ReplicationFactor = 5,
  TargetRangeSize = 6,
}

/**
 * DatabasesColumnDescriptor is used to describe metadata about an individual column
 * in the Databases table.
 */
interface DatabasesColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: DatabasesTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (s: DatabaseInfo) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // databases. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: DatabaseInfo) => any;
  // className to be applied to the td elements
  className?: string;
}

/**
 * columnDescriptors describes all columns that appear in the databases table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let columnDescriptors: DatabasesColumnDescriptor[] = [
  {
    key: DatabasesTableColumn.Name,
    title: "Database Name",
    cell: (dbInfo) => <Link to={`databases/database/${dbInfo.name}`}>{dbInfo.name}</Link>,
    sort: (dbInfo) => dbInfo.name,
    className: "expand-link", // don't pad the td element to allow the link to expand
  },
  {
    key: DatabasesTableColumn.NumTables,
    title: "# Of Tables",
    cell: (dbInfo) => dbInfo.numTables,
    sort: (dbInfo) => dbInfo.numTables,
  },
  {
    key: DatabasesTableColumn.LastModified,
    title: "Last Modified",
    cell: (dbInfo) => "", // TODO (maxlang): Pending #8246
  },
  {
    key: DatabasesTableColumn.EventsLink,
    title: "Events",
    cell: (dbInfo) => "", // TODO (maxlang): Pending #8246
  },
  {
    key: DatabasesTableColumn.ReplicationFactor,
    title: "Replication Factor",
    cell: (dbInfo) => "", // TODO (maxlang): Pending #8248
  },
  {
    key: DatabasesTableColumn.TargetRangeSize,
    title: "Target Range Size",
    cell: (dbInfo) => "", // TODO (maxlang): Pending #8248
  },
];

/******************************
 *   DATABASES MAIN COMPONENT
 */

/**
 * DatabasesMainData are the data properties which should be passed to the DatabasesMain
 * container.
 */

interface DatabasesMainData {
  // Current sort setting for the table. Incoming rows will already be sorted
  // according to this setting.
  sortSetting: SortSetting;
  // A list of databases, which are possibly sorted according to
  // sortSetting.
  sortedDatabases: DatabaseInfo[];
}

/**
 * DatabasesMainActions are the action dispatchers which should be passed to the
 * DatabasesMain container.
 */
interface DatabasesMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;

  refreshDatabases: typeof refreshDatabases;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
}

/**
 * DatabasesMainProps is the type of the props object that must be passed to
 * DatabasesMain component.
 */
type DatabasesMainProps = DatabasesMainData & DatabasesMainActions & IInjectedProps;

/**
 * DatabasesMain renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
class DatabasesMain extends React.Component<DatabasesMainProps, {}> {
  /**
   * columns is a selector which computes the input Columns to our data table,
   * based our columnDescriptors and the current sorted data
   */
  columns = createSelector(
    (props: DatabasesMainProps) => props.sortedDatabases,
    (databases: DatabaseInfo[]) => {
      return _.map(columnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(databases[index]),
          sortKey: cd.sort ? cd.key : undefined,
          className: cd.className,
        };
      });
    });

  // Callback when the user elects to change the sort setting.
  changeSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASES_SORT_SETTING_KEY, setting);
  }

  // loadDatabaseDetails loads data for each database with no info in the store.
  loadDatabaseDetails(props = this.props) {
    if (props.sortedDatabases.length) {
      _.each(props.sortedDatabases, (dbInfo) => {
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

  componentWillReceiveProps(props: DatabasesMainProps) {
    this.loadDatabaseDetails(props);
  }

  render() {
    let { sortedDatabases: databases, sortSetting } = this.props;

    if (databases) {
      return <div className="sql-table">
        <SortableTable count={databases.length}
          sortSetting={sortSetting}
          onChangeSortSetting={(setting) => this.changeSortSetting(setting)}
          columns={this.columns(this.props) } />
      </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

// Base selectors to extract data from redux state.
let databases = (state: AdminUIState): string[] => state.cachedData.databases.data  && state.cachedData.databases.data.databases;
let sortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASES_SORT_SETTING_KEY] || {};
let databaseDetails = (state: AdminUIState) => state.cachedData.databaseDetails;

// Selector which sorts statuses according to current sort setting.
let sortFunctionLookup = _(columnDescriptors).keyBy("key").mapValues<(s: DatabaseInfo) => any>("sort").value();

// Selector which generates the table rows as a DatabaseInfo[].
let databaseInfo = createSelector(
  databases,
  databaseDetails,
  (dbs: string[], details: KeyedCachedDataReducerState<DatabaseDetailsResponseMessage>): DatabaseInfo[] => _.map(dbs, (db) => new DatabaseInfo(db, details[db] && details[db].data && details[db].data.table_names.length))
);

// Selector which generates the sorted table rows as a DatabaseInfo[].
let sortedDatabases = createSelector(
  databaseInfo,
  sortSetting,
  (dbInfo: DatabaseInfo[], sort: SortSetting) => {
    let sortFn = sortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(dbInfo, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return dbInfo;
    }
  });

// Connect the DatabasesMain class with our redux store.
let databasesMainConnected = connect(
  (state: AdminUIState) => {
    return {
      sortedDatabases: sortedDatabases(state),
      sortSetting: sortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabases,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshEvents,
  }
)(DatabasesMain);

export default databasesMainConnected;
