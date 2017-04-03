import _ from "lodash";
import * as React from "react";

import * as protos from "../../js/protos";

import { AdminUIState } from "../../redux/state";
import { refreshDatabaseDetails, refreshTableDetails, refreshTableStats, generateTableID} from "../../redux/apiReducers";

import { SortSetting } from "../../components/sortabletable";

import { TableInfo } from "./data";

// DatabaseSummaryImplicitData describes properties which must be explicitly set
// on a DatabaseSummary component.
export interface DatabaseSummaryExplicitData {
  name: string;
}

// DatabaseSummaryConnectedData describes properties which are applied to a
// DatabaseSummary component by connecting to a redux store.
interface DatabaseSummaryConnectedData {
  sortSetting: SortSetting;
  tableInfos: TableInfo[];
  dbResponse: protos.cockroach.server.serverpb.DatabaseDetailsResponse;
  grants: protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant[];
}

// DatabaseSummaryActions describes actions which can be dispatched by a
// DatabaseSummary component.
interface DatabaseSummaryActions {
  setSort: (setting: SortSetting) => void;
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
export class DatabaseSummaryBase extends React.Component<DatabaseSummaryProps, {}> {
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

export function databaseDetails(state: AdminUIState) {
  return state.cachedData.databaseDetails;
}

// Function which returns TableInfo objects for all tables in a database. This
// is not a selector, because it is not based only on the Redux state - it is
// also based on the tables in a single database.
// TODO(mrtracy): look into using a component like reselect-map if this proves
// to be expensive. My current intuition is that this will not be a bottleneck.
export function tableInfos(state: AdminUIState, dbName: string) {
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
export function grants(state: AdminUIState, dbName: string) {
    let dbDetails = databaseDetails(state);
    return dbDetails[dbName] && dbDetails[dbName].data && dbDetails[dbName].data.grants;
}
