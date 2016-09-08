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
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";
import Visualization from "../../components/visualization";

type DatabaseDetailsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.DatabaseDetailsResponseMessage;
type TableDetailsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.TableDetailsResponseMessage;
type TableStatsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.TableStatsResponseMessage;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_TABLES_SORT_SETTING_KEY = "databaseDetails/sort_setting/tables";

// TableInfo is a structure which contains basic information about a Table, as
// computed from multiple backend queries.
class TableInfo {
  constructor(
    public name: string,
    public numColumns: number,
    public numIndices: number,
    public size: number
  ) { };
}

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const DatabaseTableListSortedTable = SortedTable as new () => SortedTable<TableInfo>;

/**
 * DatabaseDetailsData are the data properties which should be passed to the DatabaseDetails
 * container.
 */
interface DatabaseDetailsData {
  // Current sort setting for the table list.
  tablesSortSetting: SortSetting;
  // A list of TableInfo for the tables in the selected database.
  tableInfos: TableInfo[];
}

/**
 * DatabaseDetailsActions are the action dispatchers which should be passed to the
 * DatabaseDetails container.
 */
interface DatabaseDetailsActions {
  // Call if the user indicates they wish to change the sort of the table data.
  setUISetting: typeof setUISetting;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
}

/**
 * DatabaseDetailsProps is the type of the props object that must be passed to
 * DatabaseDetails component.
 */
type DatabaseDetailsProps = DatabaseDetailsData & DatabaseDetailsActions & IInjectedProps;

/**
 * DatabaseDetails renders the main content of the database details page.
 */
class DatabaseDetails extends React.Component<DatabaseDetailsProps, {}> {
  // Callback when the user elects to change the table table sort setting.
  changeTableSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_TABLES_SORT_SETTING_KEY, setting);
  }

  // loadTableDetails loads data for each table with no info in the store.
  loadTableDetails(props = this.props) {
    if (props.tableInfos.length) {
      _.each(props.tableInfos, (tblInfo) => {
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

  componentWillReceiveProps(props: DatabaseDetailsProps) {
    this.loadTableDetails(props);
  }

  render() {
    let { tableInfos, tablesSortSetting } = this.props;
    let dbID = this.props.params[databaseNameAttr];

    let numTables = this.props.tableInfos.length;

    if (tableInfos) {
      return <div className="sql-table">
                <div className="table-stats small half">
          <Visualization
            title={ (numTables === 1) ? "Table" : "Tables" }
            tooltip="The total number of tables in this database.">
            <div className="visualization">
              <div style={{zoom:1.0}} className="number">{ d3.format("s")(numTables) }</div>
            </div>
          </Visualization>
          <Visualization title="Database Size" tooltip="Not yet implemented.">
            <div className="visualization">
              <div style={{zoom:0.4}} className="number">
                { Bytes(_.reduce(tableInfos, (memo, t) => memo + t.size, 0)) }
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
          <DatabaseTableListSortedTable
            data={tableInfos}
            sortSetting={tablesSortSetting}
            onChangeSortSetting={(setting) => this.changeTableSortSetting(setting) }
            columns={[
            {
              title: "Table Name",
              cell: (tableInfo) => {
                return <Link to={`databases/database/${dbID}/table/${tableInfo.name}`}>{tableInfo.name}</Link>;
              },
              sort: (tableInfo) => tableInfo.name,
              className: "expand-link", // don't pad the td element to allow the link to expand
            },
            {
              title: "# of Columns",
              cell: (tableInfo) => tableInfo.numColumns,
              sort: (tableInfo) => tableInfo.numColumns,
            },
            {
              title: "# of Indices",
              cell: (tableInfo) => tableInfo.numIndices,
              sort: (tableInfo) => tableInfo.numIndices,
            },
            {
              title: "Last Modified",
              cell: (tableInfo) => "", // TODO (maxlang): Pending #8246
              sort: _.identity,
            },
            {
              title: "Last Modified By",
              cell: (tableInfo) => "", // TODO (maxlang): Pending #8246
              sort: _.identity,
            },
            ]}/>
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

// Selector which generates the table rows as a TableInfo[].
let tableInfos = createSelector(
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

// Connect the DatabaseDetails class with our redux store.
let databaseDetailsConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      tableInfos: tableInfos(state, ownProps),
      tablesSortSetting: tablesSortSetting(state),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  }
)(DatabaseDetails);

export default databaseDetailsConnected;
