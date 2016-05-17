/// <reference path="../../../typings/main.d.ts" />
import * as React from "react";
import * as _ from "lodash";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import { createSelector } from "reselect";
import { Link } from "react-router";

import { setUISetting } from "../../redux/ui";
import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";

import { refreshTableDetails } from "../../redux/databaseInfo";

type ColumnMessage = cockroach.server.TableDetailsResponse.ColumnMessage;
type IndexMessage = cockroach.server.TableDetailsResponse.IndexMessage;
type GrantMessage = cockroach.server.DatabaseDetailsResponse.GrantMessage;

// Constant used to store sort settings in the redux UI store.

// TODO: should this be a per database/table setting? Currently it's a per page setting.
const UI_TABLE_COLUMNS_SORT_SETTING_KEY = "tableDetails/sort_setting/columns";
const UI_TABLE_INDEXES_SORT_SETTING_KEY = "tableDetails/sort_setting/indexes";
const UI_TABLE_GRANTS_SORT_SETTING_KEY = "tableDetails/sort_setting/grants";

/******************************
 *      COLUMN COLUMN DEFINITION
 */

/**
 * DatabasesTableColumn provides an enumeration value for each column in the databases table.
 */
enum ColumnsTableColumns {
  Name = 1,
  Type,
  Nullable,
  Default_Value,
}

/**
 * DatabasesColumnDescriptor is used to describe metadata about an individual column
 * in the Databases table.
 */
interface ColumnsColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: ColumnsTableColumns;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (s: ColumnMessage) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // databases. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: ColumnMessage) => any;
}

/**
 * columnDescriptors describes all columns that appear in the databases table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let columnsColumnDescriptors: ColumnsColumnDescriptor[] = [
  // Database name column
  {
    key: ColumnsTableColumns.Name,
    title: "Name",
    cell: (c) => c.name,
    sort: (c) => c.name,
  },
  {
    key: ColumnsTableColumns.Name,
    title: "Type",
    cell: (c) => c.type,
    sort: (c) => c.type,
  },
  {
    key: ColumnsTableColumns.Name,
    title: "Nullable",
    cell: (c) => c.nullable,
    sort: (c) => c.nullable,
  },
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
 * DatabasesTableColumn provides an enumeration value for each column in the databases table.
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
 * DatabasesColumnDescriptor is used to describe metadata about an individual column
 * in the Databases table.
 */
interface IndexesColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: IndexesTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (s: IndexMessage) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // databases. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: IndexMessage) => any;
}

/**
 * columnDescriptors describes all columns that appear in the databases table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */

let indexesColumnDescriptors: IndexesColumnDescriptor[] = [
  {
    key: IndexesTableColumn.Name,
    title: "Name",
    cell: (i) => i.name,
    sort: (i) => i.name,
  },
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
  {
    key: IndexesTableColumn.Column,
    title: "Column",
    cell: (i) => i.column,
    sort: (i) => i.column,
  },
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
 * DatabasesTableColumn provides an enumeration value for each column in the databases table.
 */
enum GrantsTableColumn {
  User = 1,
  Grants,
}

/**
 * DatabasesColumnDescriptor is used to describe metadata about an individual column
 * in the Databases table.
 */
interface GrantsColumnDescriptor {
  // Enumeration key to distinguish this column from others.
  key: GrantsTableColumn;
  // Title string that should appear in the header column.
  title: string;
  // Function which generates the contents of an individual cell in this table.
  cell: (g: GrantMessage) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // databases. This will be used to sort the table according to the data in
  // this column.
  sort?: (s: GrantMessage) => string;
}

/**
 * columnDescriptors describes all columns that appear in the databases table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let grantsColumnDescriptors: GrantsColumnDescriptor[] = [
  {
    key: GrantsTableColumn.User,
    title: "User",
    cell: (grants) => grants.user,
    sort: (grants) => grants.user,
  },
  {
    key: GrantsTableColumn.Grants,
    title: "Grants",
    cell: (grants) => grants.privileges.join(", "),
    sort: (grants) => grants.privileges.join(", "),
  },
];

/******************************
 *   DATABASES MAIN COMPONENT
 */

/**
 * DatabasesMainData are the data properties which should be passed to the DatabasesMain
 * container.
 */

interface TableMainData {
  // Current sort setting for the table. Incoming rows will already be sorted
  // according to this setting.
  columnsSortSetting: SortSetting;
  indexesSortSetting: SortSetting;
  grantsSortSetting: SortSetting;
  // A list of databases, which are possibly sorted according to
  // sortSetting.
  sortedColumns: ColumnMessage[];
  sortedIndexes: IndexMessage[];

  sortedGrants: GrantMessage[];

}

/**
 * DatabasesMainActions are the action dispatchers which should be passed to the
 * DatabasesMain container.
 */
interface TableMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting(key: string, value: any): void;

  refreshTableDetails(): void;
}

interface TableSpecified {
  params: {
    database_name: string;
    table_name: string;
  };
}

/**
 * DatabasesMainProps is the type of the props object that must be passed to
 * DatabasesMain component.
 */
type TableMainProps = TableMainData & TableMainActions & TableSpecified;

/**
 * DatabasesMain renders the main content of the databases page, which is primarily a
 * data table of all databases.
 */
class TableMain extends React.Component<TableMainProps, {}> {
  /**
   * columns is a selector which computes the input Columns to our data table,
   * based our columnDescriptors and the current sorted data
   */
  columnColumns = createSelector(
    (props: TableMainProps) => props.sortedColumns,
    (columns: ColumnMessage[]) => {
      return _.map(columnsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(columns[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  indexesColumns = createSelector(
    (props: TableMainProps) => props.sortedIndexes,
    (indexes: IndexMessage[]) => {
      return _.map(indexesColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(indexes[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  grantColumns = createSelector(
    (props: TableMainProps) => props.sortedGrants,
    (grants: GrantMessage[]) => {
      return _.map(grantsColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(grants[index]),
          sortKey: cd.sort ? cd.key : undefined,
        };
      });
    });

  // TODO (maxlang): truncate long db names
  static title(props: any) {
    return <h2><Link to="/databases" >Databases </Link>: <Link to={`/databases/${props.params.database_name}`}> {props.params.database_name}</Link>: {props.params.table_name}</h2>;
  }

  // Callback when the user elects to change the sort setting.
  changeColumnSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_TABLE_COLUMNS_SORT_SETTING_KEY, setting);
  }

  changeIndexSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_TABLE_INDEXES_SORT_SETTING_KEY, setting);
  }

  // Callback when the user elects to change the sort setting.
  changeGrantSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_TABLE_GRANTS_SORT_SETTING_KEY, setting);
  }

  componentWillMount() {
    // Refresh databases when mounting.
    this.props.refreshTableDetails();
  }

  componentWillReceiveProps(props: TableMainProps) {
    // Refresh databases when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    // this.props.refreshDatabaseDetails();
  }

  render() {
    let { sortedColumns, sortedIndexes, sortedGrants, columnsSortSetting, indexesSortSetting, grantsSortSetting } = this.props;
    let content: React.ReactNode = null;

    if (sortedColumns) {
      content =
        <div>
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
let columns = (state: any, props: any): ColumnMessage[] => _.get(props, "params.database_name") &&
    _.get<ColumnMessage[]>(state, `databaseInfo.tableDetails["${props.params.database_name}.${props.params.table_name}"].data.columns`) || [];
let indexes = (state: any, props: any): IndexMessage[] => _.get(props, "params.database_name") &&
    _.get<IndexMessage[]>(state, `databaseInfo.tableDetails["${props.params.database_name}.${props.params.table_name}"].data.indexes`) || [];
let grants = (state: any, props: any): GrantMessage[] => _.get(props, "params.database_name") &&
  _.get<GrantMessage[]>(state, `databaseInfo.tableDetails["${props.params.database_name}.${props.params.table_name}"].data.grants`) || [];
let columnsSortSetting = (state: any): SortSetting => state.ui[UI_TABLE_COLUMNS_SORT_SETTING_KEY] || {};
let indexesSortSetting = (state: any): SortSetting => state.ui[UI_TABLE_INDEXES_SORT_SETTING_KEY] || {};
let grantsSortSetting = (state: any): SortSetting => state.ui[UI_TABLE_GRANTS_SORT_SETTING_KEY] || {};

// Selector which sorts statuses according to current sort setting.
let columnsSortFunctionLookup = _(columnsColumnDescriptors).keyBy("key").mapValues<(s: ColumnMessage) => any>("sort").value();
let indexesSortFunctionLookup = _(indexesColumnDescriptors).keyBy("key").mapValues<(s: IndexMessage) => any>("sort").value();
let grantsSortFunctionLookup = _(grantsColumnDescriptors).keyBy("key").mapValues<(s: GrantMessage) => any>("sort").value();

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

let sortedIndexes = createSelector(
  indexes,
  indexesSortSetting,
  (i, sort) => {
    console.log("I", i);
    let sortFn = indexesSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(i, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return i;
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
      sortedColumns: sortedColumns(state, ownProps),
      sortedIndexes: sortedIndexes(state, ownProps),
      sortedGrants: sortedGrants(state, ownProps),
      columnsSortSetting: columnsSortSetting(state),
      indexesSortSetting: indexesSortSetting(state),
      grantsSortSetting: grantsSortSetting(state),
    };
  },
  (dispatch: Dispatch, ownProps: any) => {
    return {
      setUISetting: function (key: string, value: any) { dispatch(setUISetting.apply(undefined, arguments)); },
      refreshTableDetails: function () {
        dispatch((() => refreshTableDetails(ownProps.params.database_name, ownProps.params.table_name)).apply(undefined, arguments));
      },
    };
  }
)(TableMain);

export default databaseMainConnected;
