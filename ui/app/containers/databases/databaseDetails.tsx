import * as _ from "lodash";
import * as React from "react";
import { Link, IInjectedProps } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { databaseName } from "../../util/constants";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabaseDetails } from "../../redux/databaseInfo";

import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

type Grant = cockroach.server.serverpb.DatabaseDetailsResponse.Grant;
type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_TABLES_SORT_SETTING_KEY = "databaseDetails/sort_setting/tables";
const UI_DATABASE_GRANTS_SORT_SETTING_KEY = "databaseDetails/sort_setting/grants";

/******************************
 *      TABLE COLUMN DEFINITION
 */

/**
 * TablesTableColumn provides an enumeration value for each column in the tables table.
 */
enum TablesTableColumn {
  Name = 1,
}

/**
 * TablesColumnDescriptor is used to describe metadata about an individual column
 * in the Tables table.
 */
interface TablesColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: TablesTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (s: string, id: string) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // tables. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: string) => any;
  // className to be applied to the td elements
  className?: string;
}

/**
 * tablesColumnDescriptors describes all columns that appear in the tables table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let tablesColumnDescriptors: TablesColumnDescriptor[] = [
  // Table name column
  {
    key: TablesTableColumn.Name,
    title: "Table Name",
    cell: (s, id) => {
      return <Link to={`databases/${id}/${s}`}>{s}</Link>;
    } ,
    sort: _.identity,
    className: "expand-link",
  },
];

/******************************
 *      GRANT COLUMN DEFINITION
 */

/**
 * GrantsTableColumn provides an enumeration value for each column in the grants table.
 */
enum GrantsTableColumn {
  User = 1,
  Grants,
}

/**
 * GrantsColumnDescriptor is used to describe metadata about an individual column
 * in the Grants table.
 */
interface GrantsColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: GrantsTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (g: Grant) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // Grants. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: Grant) => string;
}

/**
 * grantsColumnDescriptors describes all columns that appear in the grants table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let grantsColumnDescriptors: GrantsColumnDescriptor[] = [
// user column
  {
    key: GrantsTableColumn.User,
    title: "User",
    cell: (grants) => grants.user,
    sort: (grants) => grants.user,
  },
// grant list column
  {
    key: GrantsTableColumn.Grants,
    title: "Grants",
    cell: (grants) => grants.privileges.join(", "),
    sort: (grants) => grants.privileges.join(", "),
  },
];

/******************************
 *   DATABASE MAIN COMPONENT
 */

/**
 * DatabaseMainData are the data properties which should be passed to the DatabaseMain
 * container.
 */

interface DatabaseMainData {
  // Current sort setting for the table table. Incoming rows will already be sorted
  // according to this setting.
  tablesSortSetting: SortSetting;
  // Current sort setting for the grant table. Incoming rows will already be sorted
  // according to this setting.
  grantsSortSetting: SortSetting;
  // A list of tables, which are possibly sorted according to
  // sortSetting.
  sortedTables: string[];
  // A list of grants, which are possibly sorted according to
  // sortSetting.
  sortedGrants: Grant[];
}

/**
 * DatabaseMainActions are the action dispatchers which should be passed to the
 * DatabaseMain container.
 */
interface DatabaseMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
  // Call when the data needs to be refreshed.
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
}

/**
 * DatabaseMainProps is the type of the props object that must be passed to
 * DatabaseMain component.
 */
type DatabaseMainProps = DatabaseMainData & DatabaseMainActions & IInjectedProps;

/**
 * DatabaseMain renders the main content of the database details page, which is primarily a
 * data table of all tables and grants.
 */
class DatabaseMain extends React.Component<DatabaseMainProps, {}> {
  /**
   * tableColumns is a selector which computes the input Columns to the table table,
   * based on the tableColumnDescriptors and the current sorted table data
   */
  tableColumns = createSelector(
    (props: DatabaseMainProps) => props.sortedTables,
    (tables: string[]) => {
      return _.map(tablesColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(tables[index], this.props.params[databaseName]),
          sortKey: cd.sort ? cd.key : undefined,
          className: cd.className,
        };
      });
    });

  /**
   * grantColumns is a selector which computes the input Columns to the grants table,
   * based on the tableColumnDescriptors and the current sorted table data
   */
  grantColumns = createSelector(
    (props: DatabaseMainProps) => props.sortedGrants,
    (grants: Grant[]) => {
      return _.map(grantsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(grants[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  static title(props: IInjectedProps) {
    return <h2><Link to="/databases" >Databases </Link>: { props.params[databaseName] }</h2>;
  }

  // Callback when the user elects to change the table table sort setting.
  changeTableSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_TABLES_SORT_SETTING_KEY, setting);
  }

  // Callback when the user elects to change the grant table sort setting.
  changeGrantSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_GRANTS_SORT_SETTING_KEY, setting);
  }

  // refresh when the component is mounted
  componentWillMount() {
    this.props.refreshDatabaseDetails(this.props.params[databaseName]);
  }

  render() {
    let { sortedTables, sortedGrants, tablesSortSetting, grantsSortSetting } = this.props;

    if (sortedTables && sortedGrants) {
      return <div className="sql-table">
          <h2> Tables </h2>
          <SortableTable count={sortedTables.length}
            sortSetting={tablesSortSetting}
            onChangeSortSetting={(setting) => this.changeTableSortSetting(setting) }>
            {this.tableColumns(this.props) }
          </SortableTable>
          <h2> Grants </h2>
          <SortableTable count={sortedGrants.length}
            sortSetting={grantsSortSetting}
            onChangeSortSetting={(setting) => this.changeGrantSortSetting(setting) }>
            {this.grantColumns(this.props) }
          </SortableTable>
        </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

// Helper function that gets a DatabaseDetailsResponseMessage given a state and props
function getDatabaseDetails(state: AdminUIState, props: IInjectedProps): DatabaseDetailsResponseMessage {
  let details = state.databaseInfo.databaseDetails[props.params[databaseName]];
  return details && details.data;
}

// Base selectors to extract data from redux state.
let tables = (state: AdminUIState, props: IInjectedProps): string[] => getDatabaseDetails(state, props) ? getDatabaseDetails(state, props).table_names : [];
let grants = (state: AdminUIState, props: IInjectedProps): Grant[] => getDatabaseDetails(state, props) ? getDatabaseDetails(state, props).grants : [];
let tablesSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_TABLES_SORT_SETTING_KEY] || {};
let grantsSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_GRANTS_SORT_SETTING_KEY] || {};

// Selectors which sorts statuses according to current sort setting.
let tablesSortFunctionLookup = _(tablesColumnDescriptors).keyBy("key").mapValues<(s: string) => any>("sort").value();
let grantsSortFunctionLookup = _(grantsColumnDescriptors).keyBy("key").mapValues<(s: Grant) => any>("sort").value();

// Sorted tableNames
let sortedTables = createSelector(
  tables,
  tablesSortSetting,
  (t, sort) => {
    let sortFn = tablesSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(t, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return t;
    }
  });

// Sorted grants
let sortedGrants = createSelector(
  grants,
  grantsSortSetting,
  (g, sort) => {
    let sortFn = grantsSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(g, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return g;
    }
  });

// Connect the DatabaseMain class with our redux store.
let databaseMainConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      sortedTables: sortedTables(state, ownProps),
      sortedGrants: sortedGrants(state, ownProps),
      tablesSortSetting: tablesSortSetting(state),
      grantsSortSetting: grantsSortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
  }
)(DatabaseMain);

export default databaseMainConnected;
