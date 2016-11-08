import _ from "lodash";
import * as React from "react";
import { connect } from "react-redux";
import { Link } from "react-router";

import * as protos from "../../js/protos";

import { SummaryBar, SummaryItem } from "../../components/summaryBar";
import { SortSetting } from "../../components/sortabletable";
import { SortedTable } from "../../components/sortedtable";

import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshDatabaseDetails, refreshTableDetails, refreshTableStats, generateTableID} from "../../redux/apiReducers";

import { Bytes } from "../../util/format";

import { TableInfo } from "./data";

type DatabaseDetailsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.DatabaseDetailsResponseMessage;
type Grant = Proto2TypeScript.cockroach.server.serverpb.DatabaseDetailsResponse.Grant;

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
export const DatabaseGrantsSortedTable = SortedTable as new () => SortedTable<Grant>;

// Constants used to store per-page sort settings in the redux UI store.
const UI_DATABASE_TABLES_SORT_SETTING_KEY = "databases/sort_setting/tables";
const UI_DATABASE_GRANTS_SORT_SETTING_KEY = "databases/sort_setting/grants";

// Specialization of generic SortedTable component:
//   https://github.com/Microsoft/TypeScript/issues/3960
//
// The variable name must start with a capital letter or TSX will not recognize
// it as a component.
// tslint:disable-next-line:variable-name
const DatabaseTableListSortedTable = SortedTable as new () => SortedTable<TableInfo>;

// DatabaseSummaryImplicitData describes properties which must be explicitly set 
// on a DatabaseSummary component.
interface DatabaseSummaryExplicitData {
  name: string;
}

// DatabaseSummaryConnectedData describes properties which are applied to a
// DatabaseSummary component by connecting to a redux store.
interface DatabaseSummaryConnectedData {
  sortSetting: SortSetting;
  tableInfos: TableInfo[];
  dbResponse: DatabaseDetailsResponseMessage;
  grants: Grant[];
}

// DatabaseSummaryActions describes actions which can be dispatched by a
// DatabaseSummary component.
interface DatabaseSummaryActions {
  setUISetting: typeof setUISetting;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
}

type DatabaseSummaryProps = DatabaseSummaryExplicitData & DatabaseSummaryConnectedData & DatabaseSummaryActions;

// DatabaseSummaryBase implements common lifecycle methods for DatabaseSummary
// components, which differ primarily by their render() method. 
// TODO(mrtracy): We need to find a better abstraction for the common
// "refresh-on-mount-or-receiveProps" we have in many of our connected 
// components; that would allow us to avoid this inheritance.
class DatabaseSummaryBase extends React.Component<DatabaseSummaryProps, {}> {
  // loadTableDetails loads data for each table which have no info in the store.
  // TODO(mrtracy): Should this be refreshing data always? Not sure if there
  // is a performance concern with invalidation periods.
  loadTableDetails(props = this.props) {
    if (props.tableInfos && props.tableInfos.length > 0) {
      _.each(props.tableInfos, (tblInfo) => {
        if (_.isUndefined(tblInfo.numColumns)) {
          props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({
            database: props.name,
            table: tblInfo.name,
          }));
        }
        if (_.isUndefined(tblInfo.size)) {
          props.refreshTableStats(new protos.cockroach.server.serverpb.TableStatsRequest({
            database: props.name,
            table: tblInfo.name,
          }));
        }
      });
    }
  }

  // Refresh when the component is mounted.
  componentWillMount() {
    this.props.refreshDatabaseDetails(new protos.cockroach.server.serverpb.DatabaseDetailsRequest({ database: this.props.name }));
    this.loadTableDetails();
  }

  // Refresh when the component receives properties.
  componentWillReceiveProps(props: DatabaseSummaryProps) {
    this.loadTableDetails(props);
  }

  render(): React.ReactElement<any> {
      throw new Error("DatabaseSummaryBase should never be instantiated directly. ");
  }
}

// DatabaseSummaryTables displays a summary section describing the tables
// contained in a single database.
class DatabaseSummaryTables extends DatabaseSummaryBase {
  // Callback when the user elects to change the table table sort setting.
  changeTableSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_TABLES_SORT_SETTING_KEY, setting);
  }

  totalSize() {
    let tableInfos = this.props.tableInfos;
    return _.sumBy(tableInfos, (ti) => ti.size);
  }

  totalRangeCount() {
    let tableInfos = this.props.tableInfos;
    return _.sumBy(tableInfos, (ti) => ti.rangeCount);
  }

  render() {
    let { tableInfos, sortSetting } = this.props;
    let dbID = this.props.name;

    let numTables = tableInfos && tableInfos.length || 0;

    return <div className="database-summary">
      <div className="database-summary-title">
        { dbID }
      </div>
      <div className="content">
        <div className="database-summary-table sql-table">
        {
          (numTables === 0) ? "" :
          <DatabaseTableListSortedTable
              data={tableInfos}
              sortSetting={sortSetting}
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
                title: "Size",
                cell: (tableInfo) => Bytes(tableInfo.size),
                sort: (tableInfo) => tableInfo.size,
              },
              {
                title: "Ranges",
                cell: (tableInfo) => tableInfo.rangeCount,
                sort: (tableInfo) => tableInfo.rangeCount,
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
                title: "Schema Change",
                cell: (tableInfo) => "",
              },
              ]}/>
        }
        </div>
      </div>
      <SummaryBar>
        <SummaryItem
          title="Database Size"
          tooltip="Total disk size of this database."
          value={ this.totalSize() }
          format={ Bytes }/>
        <SummaryItem
          title={ (numTables === 1) ? "Table" : "Tables" }
          tooltip="The total number of tables in this database."
          value={ numTables }/>
        <SummaryItem
          title="Total Range Count"
          tooltip="The total ranges across all tables in this database."
          value={ this.totalRangeCount() }/>
      </SummaryBar>
    </div>;
  }
}

// DatabaseSummaryGrants displays a summary section describing the grants
// active on a single database.
class DatabaseSummaryGrants extends DatabaseSummaryBase {
  // Callback when the user elects to change the table table sort setting.
  changeTableSortSetting(setting: SortSetting) {
    this.props.setUISetting(UI_DATABASE_GRANTS_SORT_SETTING_KEY, setting);
  }

  totalUsers() {
    let grants = this.props.grants;
    return grants && grants.length;
  }

  render() {
    let { grants, sortSetting } = this.props;
    let dbID = this.props.name;

    let numTables = tableInfos && tableInfos.length || 0;

    return <div className="database-summary">
      <div className="database-summary-title">
        { dbID }
      </div>
      <div className="content">
        <div className="database-summary-table sql-table">
        {
          (numTables === 0) ? "" :
          <DatabaseGrantsSortedTable
              data={grants}
              sortSetting={sortSetting}
              onChangeSortSetting={(setting) => this.changeTableSortSetting(setting) }
              columns={[
                {
                    title: "User",
                    cell: (grant) => grant.user,
                    sort: (grant) => grant.user,
                },
                {
                    title: "Grants",
                    cell: (grant) => grant.privileges.join(", "),
                },
              ]}/>
        }
        </div>
      </div>
      <SummaryBar>
        <SummaryItem
          title="Total Users"
          tooltip="Total users that have been granted permissions on this table."
          value={ this.totalUsers() }/>
      </SummaryBar>
    </div>;
  }
}

// Base selectors to extract data from redux state.
let databaseDetails = (state: AdminUIState) => state.cachedData.databaseDetails;
let tablesSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_TABLES_SORT_SETTING_KEY] || {};
let grantsSortSetting = (state: AdminUIState): SortSetting => state.ui[UI_DATABASE_GRANTS_SORT_SETTING_KEY] || {};

// Function which returns TableInfo objects for all tables in a database. This
// is not a selector, because it is not based only on the Redux state - it is
// also based on the tables in a single database.
// TODO(mrtracy): look into using a component like reselect-map if this proves
// to be expensive. My current intuition is that this will not be a bottleneck.
function tableInfos(state: AdminUIState, dbName: string) {
  let dbDetails = databaseDetails(state);
  let tableNames = dbDetails[dbName] && dbDetails[dbName].data && dbDetails[dbName].data.table_names;
  if (!tableNames) {
    return null;
  }
  let details = state.cachedData.tableDetails;
  let stats =  state.cachedData.tableStats;
  return _.map(tableNames, (tableName) => {
    let tblId = generateTableID(dbName, tableName);
    let tblDetails = details[tblId] && details[tblId].data;
    let tblStats = stats[tblId] && stats[tblId].data;
    return new TableInfo(tableName, tblDetails, tblStats);
  });
}

// Function which extracts the grants for a single database from redux state.
function grants(state: AdminUIState, dbName: string) {
    let dbDetails = databaseDetails(state);
    return dbDetails[dbName] && dbDetails[dbName].data && dbDetails[dbName].data.grants;
}

// Connect the DatabaseSummaryTables class with redux store.
let databaseSummaryTablesConnected = connect(
  (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
    return {
      tableInfos: tableInfos(state, ownProps.name),
      sortSetting: tablesSortSetting(state),
      dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
      grants: grants(state, ownProps.name),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  }
)(DatabaseSummaryTables);

// Connect the DatabaseSummaryGrants class with our redux store.
let databaseSummaryGrantsConnected = connect(
  (state: AdminUIState, ownProps: DatabaseSummaryExplicitData) => {
    return {
      tableInfos: tableInfos(state, ownProps.name),
      sortSetting: grantsSortSetting(state),
      dbResponse: databaseDetails(state)[ownProps.name] && databaseDetails(state)[ownProps.name].data,
      grants: grants(state, ownProps.name),
    };
  },
  {
    setUISetting,
    refreshDatabaseDetails,
    refreshTableDetails,
    refreshTableStats,
  }
)(DatabaseSummaryGrants);

export {
    databaseSummaryTablesConnected as DatabaseSummaryTables,
    databaseSummaryGrantsConnected as DatabaseSummaryGrants,
}
