import * as _ from "lodash";
import * as React from "react";
import { Link, IInjectedProps } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { databaseName, tableName } from "../../util/constants";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshTableDetails, generateTableID } from "../../redux/databaseInfo";

import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

type Column = cockroach.server.serverpb.TableDetailsResponse.Column;
type Index = cockroach.server.serverpb.TableDetailsResponse.Index;
type Grant = cockroach.server.serverpb.DatabaseDetailsResponse.Grant;
type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;

// Constants used to store sort settings in the redux UI store.

// TODO: should this be a per database/table setting? Currently it's a per page setting.
const UI_TABLE_COLUMNS_SORT_SETTING_KEY = "tableDetails/sort_setting/columns";
const UI_TABLE_INDEXES_SORT_SETTING_KEY = "tableDetails/sort_setting/indexes";
const UI_TABLE_GRANTS_SORT_SETTING_KEY = "tableDetails/sort_setting/grants";

/******************************
 *      COLUMN COLUMN DEFINITION
 */

/**
 * ColumnsTableColumns provides an enumeration value for each column in the columns table.
 */
enum ColumnsTableColumns {
  Name = 1,
  Type,
  Nullable,
  Default_Value,
}

/**
 * ColumnsColumnDescriptor is used to describe metadata about an individual column
 * in the Columns table.
 */
interface ColumnsColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: ColumnsTableColumns;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (s: Column) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // columns. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: Column) => any;
}

/**
 * columnsColumnDescriptors describes all columns that appear in the columns table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let columnsColumnDescriptors: ColumnsColumnDescriptor[] = [
  // Column name column
  {
    key: ColumnsTableColumns.Name,
    title: "Name",
    cell: (c) => c.name,
    sort: (c) => c.name,
  },
  // Column type column
  {
    key: ColumnsTableColumns.Name,
    title: "Type",
    cell: (c) => c.type,
    sort: (c) => c.type,
  },
  // Nullable column
  {
    key: ColumnsTableColumns.Name,
    title: "Nullable",
    cell: (c) => c.nullable,
    sort: (c) => c.nullable,
  },
  // Default value column
  {
    key: ColumnsTableColumns.Name,
    title: "Default Value",
    cell: (c) => c.default_value,
    sort: (c) => c.default_value,
  },
];

/******************************
 *      INDEX COLUMN DEFINITION
 */

/**
 * IndexesTableColumn provides an enumeration value for each column in the indexes table.
 */
enum IndexesTableColumn {
  Name = 1,
  Unique,
  Seq,
  Column,
  Direction,
  Storing,
}

/**
 * IndexesColumnDescriptor is used to describe metadata about an individual column
 * in the Indexes table.
 */
interface IndexesColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: IndexesTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (s: Index) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // databases. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: Index) => any;
}

/**
 * indexesColumnDescriptors describes all columns that appear in the indexes table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */

let indexesColumnDescriptors: IndexesColumnDescriptor[] = [
  // Index name column
  {
    key: IndexesTableColumn.Name,
    title: "Name",
    cell: (i) => i.name,
    sort: (i) => i.name,
  },
  // Unique column
  {
    key: IndexesTableColumn.Unique,
    title: "Unique",
    cell: (i) => i.unique,
    sort: (i) => i.unique,
  },
  {
    key: IndexesTableColumn.Seq,
    title: "Seq",
    cell: (i) => i.seq.toString(),
    sort: (i) => i.seq.toString(),
  },
  // Which column the index is tied to
  {
    key: IndexesTableColumn.Column,
    title: "Column",
    cell: (i) => i.column,
    sort: (i) => i.column,
  },
  // Index direction
  {
    key: IndexesTableColumn.Direction,
    title: "Direction",
    cell: (i) => i.direction,
    sort: (i) => i.direction,
  },
  {
    key: IndexesTableColumn.Storing,
    title: "Storing",
    cell: (i) => i.storing,
    sort: (i) => i.storing,
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
  // grants. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: Grant) => string;
}

/**
 * grantsColumnDescriptors describes all columns that appear in the grants table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let grantsColumnDescriptors: GrantsColumnDescriptor[] = [
  // User
  {
    key: GrantsTableColumn.User,
    title: "User",
    cell: (grants) => grants.user,
    sort: (grants) => grants.user,
  },
  // List of Grants
  {
    key: GrantsTableColumn.Grants,
    title: "Grants",
    cell: (grants) => grants.privileges.join(", "),
    sort: (grants) => grants.privileges.join(", "),
  },
];

/******************************
 *   TABLE DETAILS MAIN COMPONENT
 */

/**
 * TableMainData are the data properties which should be passed to the TableMain
 * container.
 */

interface TableMainData {
  // Current sort settings for the tables. Incoming rows will already be sorted
  // according to this setting.
  columnsSortSetting: SortSetting;
  indexesSortSetting: SortSetting;
  grantsSortSetting: SortSetting;
  // A list of columns, indexes and grants, which are each possibly sorted according to
  // sortSetting.
  sortedColumns: Column[];
  sortedIndexes: Index[];
  sortedGrants: Grant[];
}

/**
 * TableMainActions are the action dispatchers which should be passed to the
 * TableMain container.
 */
interface TableMainActions {
  // Call if the user indicates they wish to change the sort of any table data.
  setUISetting: typeof setUISetting;
  // Refresh the table data
  refreshTableDetails: typeof refreshTableDetails;
}

/**
 * TableMainProps is the type of the props object that must be passed to
 * TableMain component.
 */
type TableMainProps = TableMainData & TableMainActions & IInjectedProps;

/**
 * TableMain renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
class TableMain extends React.Component<TableMainProps, {}> {
  /**
   * columnsColumns is a selector which computes the input Columns to our data table,
   * based our columnsColumnDescriptors and the current sorted data
   */
  columnColumns = createSelector(
    (props: TableMainProps) => props.sortedColumns,
    (columns: Column[]) => {
      return _.map(columnsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(columns[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  /**
   * indexesColumns is a selector which computes the input Columns to our data table,
   * based our indexesColumnDescriptors and the current sorted data
   */
  indexesColumns = createSelector(
    (props: TableMainProps) => props.sortedIndexes,
    (indexes: Index[]) => {
      return _.map(indexesColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(indexes[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  /**
   * grantColumns is a selector which computes the input Columns to our data table,
   * based our grantsColumnDescriptors and the current sorted data
   */
  grantColumns = createSelector(
    (props: TableMainProps) => props.sortedGrants,
    (grants: Grant[]) => {
      return _.map(grantsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(grants[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  // TODO(maxlang): truncate long db names
  static title(props: IInjectedProps) {
    return <h2><Link to="/databases" >Databases </Link>: <Link to={`/databases/${props.params[databaseName]}`}> {props.params[databaseName]}</Link>: {props.params[tableName]}</h2>;
  }

  // Callbacks when the user elects to change the sort settings.
  changeColumnSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_TABLE_COLUMNS_SORT_SETTING_KEY, setting);
  }
  changeIndexSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_TABLE_INDEXES_SORT_SETTING_KEY, setting);
  }
  changeGrantSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_TABLE_GRANTS_SORT_SETTING_KEY, setting);
  }

  componentWillMount() {
    // Refresh databases when mounting.
    this.props.refreshTableDetails(this.props.params[databaseName], this.props.params[tableName]);
  }

  render() {
    let { sortedColumns, sortedIndexes, sortedGrants, columnsSortSetting, indexesSortSetting, grantsSortSetting } = this.props;

    if (sortedColumns && sortedIndexes && sortedGrants) {
      return <div className="sql-table">
        <h2> Columns </h2>
        <SortableTable count={sortedColumns.length}
          sortSetting={columnsSortSetting}
          onChangeSortSetting={(setting) => this.changeColumnSortSetting(setting) }>
          {this.columnColumns(this.props) }
        </SortableTable>
        <h2> Indexes </h2>
        <SortableTable count={sortedIndexes.length}
          sortSetting={indexesSortSetting}
          onChangeSortSetting={(setting) => this.changeIndexSortSetting(setting) }>
          {this.indexesColumns(this.props) }
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

// Helper function that gets a TableDetailsResponseMessage given a state and props
function getTableDetails(state: AdminUIState, props: IInjectedProps): TableDetailsResponseMessage {
  let db = props.params[databaseName];
  let table = props.params[tableName];
  let details = state.databaseInfo.tableDetails[generateTableID(db, table)];
  return details && details.data;
}

// Base selectors to extract data from redux state.
let columns = (state: AdminUIState, props: IInjectedProps): Column[] => getTableDetails(state, props) ? getTableDetails(state, props).getColumns() : [];
let indexes = (state: AdminUIState, props: IInjectedProps): Index[] => getTableDetails(state, props) ? getTableDetails(state, props).getIndexes() : [];
let grants = (state: AdminUIState, props: IInjectedProps): Grant[] => getTableDetails(state, props) ? getTableDetails(state, props).getGrants() : [];
let columnsSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_TABLE_COLUMNS_SORT_SETTING_KEY] || {};
let indexesSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_TABLE_INDEXES_SORT_SETTING_KEY] || {};
let grantsSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_TABLE_GRANTS_SORT_SETTING_KEY] || {};

// Selector which sorts statuses according to current sort setting.
let columnsSortFunctionLookup = _(columnsColumnDescriptors).keyBy("key").mapValues<(s: Column) => any>("sort").value();
let indexesSortFunctionLookup = _(indexesColumnDescriptors).keyBy("key").mapValues<(s: Index) => any>("sort").value();
let grantsSortFunctionLookup = _(grantsColumnDescriptors).keyBy("key").mapValues<(s: Grant) => any>("sort").value();

// sorted columns
let sortedColumns = createSelector(
  columns,
  columnsSortSetting,
  (c, sort) => {
    let sortFn = columnsSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(c, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return c;
    }
  });

// sorted indexes
let sortedIndexes = createSelector(
  indexes,
  indexesSortSetting,
  (i, sort) => {
    let sortFn = indexesSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(i, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return i;
    }
  });

// sorted grants
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

// Connect the TableMain class with our redux store.
let tableMainConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      sortedColumns: sortedColumns(state, ownProps),
      sortedIndexes: sortedIndexes(state, ownProps),
      sortedGrants: sortedGrants(state, ownProps),
      columnsSortSetting: columnsSortSetting(state),
      indexesSortSetting: indexesSortSetting(state),
      grantsSortSetting: grantsSortSetting(state),
    };
  },
  {
    setUISetting,
    refreshTableDetails,
  }
)(TableMain);

export default tableMainConnected;
