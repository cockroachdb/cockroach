import _ from "lodash";
import * as React from "react";
import * as d3 from "d3";
import { Link, IInjectedProps } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import * as protos from "../../js/protos";
import { databaseNameAttr } from "../../util/constants";
import { Bytes } from "../../util/format";
import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabaseDetails, refreshTableDetails, refreshTableStats, generateTableID, KeyedCachedDataReducerState } from "../../redux/apiReducers";
import { SortableTable, SortableColumn, SortSetting } from "../../components/sortabletable";
import Visualization from "../../components/visualization";

type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;
type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;
type TableStatsResponseMessage = cockroach.server.serverpb.TableStatsResponseMessage;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_TABLES_SORT_SETTING_KEY = "databaseDetails/sort_setting/tables";
const UI_DATABASE_GRANTS_SORT_SETTING_KEY = "databaseDetails/sort_setting/grants";

class TableInfo {
  constructor(
    public name: string,
    public numColumns: number,
    public numIndices: number,
    public size: number
  ) { };
}

/******************************
 *      TABLE COLUMN DEFINITION
 */

/**
 * TablesTableColumn provides an enumeration value for each column in the tables table.
 */
enum TablesTableColumn {
  Name = 1,
  NumColumns = 2,
  NumIndices = 3,
  LastModified = 4,
  LastModifiedBy = 5,
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
  cell: (tableInfo: TableInfo, id: string) => React.ReactNode;
  // Function which returns a value that can be used to sort a collection of
  // tables. This will be used to sort the table according to the data in
  // this column.
  sort?: (tableInfo: TableInfo) => any;
  // className to be applied to the td elements
  className?: string;
}

/**
 * tablesColumnDescriptors describes all columns that appear in the tables table.
 * Columns are displayed in the same order they do in this collection, from left
 * to right.
 */
let tablesColumnDescriptors: TablesColumnDescriptor[] = [
  {
    key: TablesTableColumn.Name,
    title: "Table Name",
    cell: (tableInfo, id) => {
      return <Link to={`databases/database/${id}/table/${tableInfo.name}`}>{tableInfo.name}</Link>;
    } ,
    sort: (tableInfo) => tableInfo.name,
    className: "expand-link", // don't pad the td element to allow the link to expand
  },
  {
    key: TablesTableColumn.NumColumns,
    title: "# of Columns",
    cell: (tableInfo, id) => tableInfo.numColumns,
    sort: (tableInfo) => tableInfo.numColumns,
  },
  {
    key: TablesTableColumn.NumIndices,
    title: "# of Indices",
    cell: (tableInfo, id) => tableInfo.numIndices,
    sort: (tableInfo) => tableInfo.numIndices,
  },
  {
    key: TablesTableColumn.LastModified,
    title: "Last Modified",
    cell: (tableInfo, id) => "", // TODO (maxlang): Pending #8246
    sort: _.identity,
  },
  {
    key: TablesTableColumn.LastModifiedBy,
    title: "Last Modified By",
    cell: (tableInfo, id) => "", // TODO (maxlang): Pending #8246
    sort: _.identity,
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
  // Current sort setting for the grant table. Incoming rows will already be sorted
  // according to this setting.
  tablesSortSetting: SortSetting;
  // A list of grants, which are possibly sorted according to
  // sortSetting.
  sortedTables: TableInfo[];
}

/**
 * DatabaseMainActions are the action dispatchers which should be passed to the
 * DatabaseMain container.
 */
interface DatabaseMainActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
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
    (tables: TableInfo[]) => {
      return _.map(tablesColumnDescriptors, (cd): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(tables[index], this.props.params[databaseNameAttr]),
          sortKey: cd.sort ? cd.key : undefined,
          className: cd.className,
        };
      });
    });

  // Callback when the user elects to change the table table sort setting.
  changeTableSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_TABLES_SORT_SETTING_KEY, setting);
  }

  // Callback when the user elects to change the grant table sort setting.
  changeGrantSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_GRANTS_SORT_SETTING_KEY, setting);
  }
  // loadTableDetails loads data for each table with no info in the store.
  loadTableDetails(props = this.props) {
    if (props.sortedTables.length) {
      _.each(props.sortedTables, (tblInfo) => {
        if (_.isUndefined(tblInfo.numColumns)) {
          props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({
            database: props.params[databaseNameAttr],
            table: tblInfo.name,
          }));
        }
        if (_.isUndefined(tblInfo.size)) {
          props.refreshTableStats(new protos.cockroach.server.serverpb.TableStatsRequest({
            database: props.params[databaseNameAttr],
            table: tblInfo.name,
          }));
        }
      });
    }
  }

  // Refresh when the component is mounted.
  componentWillMount() {
    this.props.refreshDatabaseDetails(new protos.cockroach.server.serverpb.DatabaseDetailsRequest({ database: this.props.params[databaseNameAttr] }));
    this.loadTableDetails();
  }

  componentWillReceiveProps(props: DatabaseMainProps) {
    this.loadTableDetails(props);
  }

  render() {
    let { sortedTables, tablesSortSetting } = this.props;

    let numTables = this.props.sortedTables.length;

    if (sortedTables) {
      return <div className="sql-table">
                <div className="table-stats small half">
          <Visualization
            title={ (numTables === 1) ? "Table" : "Tables" }
            tooltip="The total number of tables in this database.">
            <div className="visualization">
              <div style={{zoom:"100%"}} className="number">{ d3.format("s")(numTables) }</div>
            </div>
          </Visualization>
          <Visualization title="Database Size" tooltip="Not yet implemented.">
            <div className="visualization">
              <div style={{ zoom: "40%" }} className="number">
                { Bytes(_.reduce(sortedTables, (memo, t) => memo + t.size, 0)) }
              </div>
            </div>
          </Visualization>
          <Visualization title="Replication Factor" tooltip="Not yet implemented.">
            <div className="visualization">
              <div>Not Implemented</div> {/* TODO (maxlang): Pending #8248 */}
            </div>
          </Visualization>
          <Visualization title="Target Range Size" tooltip="Not yet implemented.">
            <div className="visualization">
              <div>Not Implemented</div> {/* TODO (maxlang): Pending #8248 */}
            </div>
          </Visualization>
        </div>
          <SortableTable count={sortedTables.length}
            sortSetting={tablesSortSetting}
            onChangeSortSetting={(setting) => this.changeTableSortSetting(setting) }
            columns={ this.tableColumns(this.props) }/>
        </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

// Helper function that gets a DatabaseDetailsResponseMessage given a state and props
function databaseDetails(state: AdminUIState, props: IInjectedProps): DatabaseDetailsResponseMessage {
  let details = state.cachedData.databaseDetails[props.params[databaseNameAttr]];
  return details && details.data;
}

// Base selectors to extract data from redux state.
let tables = (state: AdminUIState, props: IInjectedProps): string[] => databaseDetails(state, props) ? databaseDetails(state, props).table_names : [];
let tablesSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_TABLES_SORT_SETTING_KEY] || {};

// Selectors which sorts statuses according to current sort setting.
let tablesSortFunctionLookup = _(tablesColumnDescriptors).keyBy("key").mapValues<(s: TableInfo) => any>("sort").value();

// Selector which generates the table rows as a TableInfo[].
let tableInfo = createSelector(
  (dummy: {}, props: IInjectedProps) => props.params[databaseNameAttr],
  tables,
  (state: AdminUIState) => state.cachedData.tableDetails,
  (state: AdminUIState) => state.cachedData.tableStats,
  (dbName: string, tableNames: string[],
    tableDetails: KeyedCachedDataReducerState<TableDetailsResponseMessage>,
    tableStats: KeyedCachedDataReducerState<TableStatsResponseMessage>): TableInfo[] => {
      return _.map(tableNames, (tableName) => {
        let curTableDetails = tableDetails[generateTableID(dbName, tableName)] && tableDetails[generateTableID(dbName, tableName)].data;
        let curTableStats = tableStats[generateTableID(dbName, tableName)] && tableStats[generateTableID(dbName, tableName)].data;
        return new TableInfo(
          tableName,
          curTableDetails && curTableDetails.columns.length,
          curTableDetails && curTableDetails.indexes.length,
          curTableStats && (curTableStats.stats.val_bytes.toNumber() + curTableStats.stats.sys_bytes.toNumber())
        );
      });
    }
);

// Selector which generates the sorted table rows as a TableInfo[].
let sortedTables = createSelector(
  tableInfo,
  tablesSortSetting,
  (t: TableInfo[], sort: SortSetting) => {
    let sortFn = tablesSortFunctionLookup[sort.sortKey];
    if (sort && sortFn) {
      return _.orderBy(t, sortFn, sort.ascending ? "asc" : "desc");
    } else {
      return t;
    }
  });

// Connect the DatabaseMain class with our redux store.
let databaseMainConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      sortedTables: sortedTables(state, ownProps),
      tablesSortSetting: tablesSortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  }
)(DatabaseMain);

export default databaseMainConnected;
