import * as React from "react";
import { Link } from "react-router";
import * as _ from "lodash";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

import { refreshDatabases } from "../../redux/databaseInfo";

// Constant used to store sort settings in the redux UI store.
const UI_DATABASES_SORT_SETTING_KEY = "databaseList/sort_setting";

/******************************
 *      COLUMN DEFINITION
 */

/**
 * DatabasesTableColumn provides an enumeration value for each column in the databases table.
 */
enum DatabasesTableColumn {
  Name = 1,
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
  cell: (s: string) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // databases. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: string) => any;
  // className to be applied to the td elements
  className?: string;
}

/**
 * columnDescriptors describes all columns that appear in the databases table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let columnDescriptors: DatabasesColumnDescriptor[] = [
  // Database name column
  {
    key: DatabasesTableColumn.Name,
    title: "Database Name",
    cell: (s) => <Link to={`databases/${s}`}>{s}</Link>,
    sort: _.identity,
    className: "expand-link", // don't pad the td element to allow the link to expand
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
  sortedDatabases: string[];
}

/**
 * DatabasesMainActions are the action dispatchers which should be passed to the
 * DatabasesMain container.
 */
interface DatabasesMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;

  refreshDatabases: typeof refreshDatabases;
}

/**
 * DatabasesMainProps is the type of the props object that must be passed to
 * DatabasesMain component.
 */
type DatabasesMainProps = DatabasesMainData & DatabasesMainActions;

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
    (databases: string[]) => {
      return _.map(columnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(databases[index]),
          sortKey: cd.sort ? cd.key : undefined,
          className: cd.className,
        };
      });
    });

  static title() {
    return <h2>Databases</h2>;
  }

  // Callback when the user elects to change the sort setting.
  changeSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASES_SORT_SETTING_KEY, setting);
  }

  componentWillMount() {
    // Refresh databases when mounting.
    this.props.refreshDatabases();
  }

  componentWillReceiveProps(props: DatabasesMainProps) {
    // Refresh databases when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    // this.props.refreshDatabases();
  }

  render() {
    let { sortedDatabases: databases, sortSetting } = this.props;

    if (databases) {
      return <div className="sql-table">
        <SortableTable count={databases.length}
          sortSetting={sortSetting}
          onChangeSortSetting={(setting) => this.changeSortSetting(setting) }>
          {this.columns(this.props) }
        </SortableTable>
      </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

// Base selectors to extract data from redux state.
let databases = (state: AdminUIState): string[] => state.databaseInfo && state.databaseInfo.databases && state.databaseInfo.databases.data  && state.databaseInfo.databases.data.databases;
let sortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASES_SORT_SETTING_KEY] || {};

// Selector which sorts statuses according to current sort setting.
let sortFunctionLookup = _(columnDescriptors).keyBy("key").mapValues<(s: string) => any>("sort").value();

let sortedDatabases = createSelector(
  databases,
  sortSetting,
  (dbs, sort) => {
    let sortFn = sortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(dbs, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return dbs;
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
  }
)(DatabasesMain);

export default databasesMainConnected;
