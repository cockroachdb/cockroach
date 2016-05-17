/// <reference path="../../../typings/main.d.ts" />
import * as React from "react";
import { Link } from "react-router";
import * as _ from "lodash";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import { createSelector } from "reselect";

import { setUISetting } from "../../redux/ui";
import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

import { refreshDatabaseDetails } from "../../redux/databaseInfo";

type GrantMessage = cockroach.server.DatabaseDetailsResponse.GrantMessage;

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
  cell: (g: GrantMessage) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // Grants. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: GrantMessage) => string;
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
  sortedGrants: GrantMessage[];
}

/**
 * DatabaseMainActions are the action dispatchers which should be passed to the
 * DatabaseMain container.
 */
interface DatabaseMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting(key: string, value: any): void;
  // Call when the data needs to be refreshed.
  refreshDatabaseDetails(): void;
}

/**
 * This augments DatabaseMainProps to add known values from the route.
 */
interface DatabaseSpecified {
  params: { database_name: string; };
}

/**
 * DatabaseMainProps is the type of the props object that must be passed to
 * DatabaseMain component.
 */
type DatabaseMainProps = DatabaseMainData & DatabaseMainActions & DatabaseSpecified;

/**
 * DatabaseMain renders the main content of the database details page, which is primarily a
 * data table of all tables and grants.
 */
class DatabaseMain extends React.Component<DatabaseMainProps, {}> {
  /**
   * tableColumns is a selector which computes the input Columns to the table table,
   * based the tableColumnDescriptors and the current sorted table data
   */
  tableColumns = createSelector(
    (props: DatabaseMainProps) => props.sortedTables,
    (tables: string[]) => {
      return _.map(tablesColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(tables[index], this.props.params.database_name),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  /**
   * grantColumns is a selector which computes the input Columns to the grants table,
   * based the tableColumnDescriptors and the current sorted table data
   */
  grantColumns = createSelector(
    (props: DatabaseMainProps) => props.sortedGrants,
    (grants: GrantMessage[]) => {
      return _.map(grantsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(grants[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  static title(props: any) {
    return <h2><Link to="/databases" >Databases </Link>: { props.params.database_name }</h2>;
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
    // Refresh databases when mounting.
    this.props.refreshDatabaseDetails();
  }

  render() {
    let { sortedTables, sortedGrants, tablesSortSetting, grantsSortSetting } = this.props;
    let content: React.ReactNode = null;

    if (sortedTables) {
      content =
        <div>
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
    } else {
      content = <div>No results.</div>;
    }

    return <div className="sql-table">
      { content }
    </div>;
  }
}

/******************************
 *         SELECTORS
 */

// Base selectors to extract data from redux state.
// TODO: _.get doesn't provide typechecking. Add typechecking back in once the
// state type import problem is solved.
let tables = (state: any, props: any): string[] => _.get(props, "params.database_name") &&
    _.get<string[]>(state, `databaseInfo.databaseDetails[${props.params.database_name}].data.table_names`) || [];
let grants = (state: any, props: any): GrantMessage[] => _.get(props, "params.database_name") &&
  _.get<GrantMessage[]>(state, `databaseInfo.databaseDetails[${props.params.database_name}].data.grants`) || [];
let tablesSortSetting = (state: any): SortSetting => state.ui[UI_DATABASE_TABLES_SORT_SETTING_KEY] || {};
let grantsSortSetting = (state: any): SortSetting => state.ui[UI_DATABASE_GRANTS_SORT_SETTING_KEY] || {};

// Selector which sorts statuses according to current sort setting.
let tablesSortFunctionLookup = _(tablesColumnDescriptors).keyBy("key").mapValues<(s: string) => any>("sort").value();
let grantsSortFunctionLookup = _(grantsColumnDescriptors).keyBy("key").mapValues<(s: GrantMessage) => any>("sort").value();

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

// Connect the DatabasesMain class with our redux store.
let databaseMainConnected = connect(
  (state, ownProps) => {
    return {
      sortedTables: sortedTables(state, ownProps),
      sortedGrants: sortedGrants(state, ownProps),
      tablesSortSetting: tablesSortSetting(state),
      grantsSortSetting: grantsSortSetting(state),
    };
  },
  (dispatch: Dispatch, ownProps: any) => {
    return {
      setUISetting: function (key: string, value: any) { dispatch(setUISetting.apply(undefined, arguments)); },
      refreshDatabaseDetails: function () {
        dispatch((() => refreshDatabaseDetails(ownProps.params.database_name)).apply(undefined, arguments));
      },
    };
  }
)(DatabaseMain);

export default databaseMainConnected;
