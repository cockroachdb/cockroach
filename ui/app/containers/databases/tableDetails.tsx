import * as React from "react";
import * as d3 from "d3";
import { IInjectedProps } from "react-router";
import { connect } from "react-redux";

import * as protos from "../../js/protos";
import { databaseNameAttr, tableNameAttr } from "../../util/constants";
import { Bytes } from "../../util/format";
import { AdminUIState } from "../../redux/state";
import { setUISetting } from "../../redux/ui";
import { refreshTableDetails, refreshTableStats, generateTableID } from "../../redux/apiReducers";
import Visualization from "../../components/visualization";

type TableDetailsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.TableDetailsResponseMessage;
type TableStatsResponseMessage = Proto2TypeScript.cockroach.server.serverpb.TableStatsResponseMessage;

/******************************
 *   TABLE DETAILS MAIN COMPONENT
 */

/**
 * TableMainData are the data properties which should be passed to the TableMain
 * container.
 */

interface TableMainData {
  tableDetails: TableDetailsResponseMessage;
  tableStats: TableStatsResponseMessage;
}

/**
 * TableMainActions are the action dispatchers which should be passed to the
 * TableMain container.
 */
interface TableMainActions {
  // Refresh the table data
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
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

  componentWillMount() {
    // Refresh databases when mounting.
    this.props.refreshTableDetails(new protos.cockroach.server.serverpb.TableDetailsRequest({ database: this.props.params[databaseNameAttr], table: this.props.params[tableNameAttr] }));
    this.props.refreshTableStats(new protos.cockroach.server.serverpb.TableStatsRequest({ database: this.props.params[databaseNameAttr], table: this.props.params[tableNameAttr] }));

  }

  render() {
    let { tableDetails, tableStats } = this.props;

    if (tableDetails) {
      let ranges = tableDetails.range_count.toNumber();
      let createTableStatement = tableDetails.create_table_statement;
      let replicationFactor = tableDetails.zone_config.replica_attrs.length;
      let targetRangeSize = tableDetails.zone_config.range_max_bytes.toNumber();
      let tableSize = tableStats && (tableStats.stats.live_bytes.toNumber() + tableStats.stats.key_bytes.toNumber());

      return <div className="sql-table">
        <div className="table-stats small half">
          <Visualization
            title={ (ranges === 1) ? "Range" : "Ranges" }
            tooltip="The total number of ranges this table spans.">
            <div className="visualization">
              <div style={{zoom:"100%"}} className="number">{ d3.format("s")(ranges) }</div>
            </div>
          </Visualization>
          <Visualization title="Table Size" tooltip="Not yet implemented.">
            <div className="visualization">
              <div style={{zoom:"40%"}} className="number">{ Bytes(tableSize) }</div>
            </div>
          </Visualization>
          <Visualization title="Replication Factor" tooltip="Not yet implemented.">
            <div className="visualization">
              <div style={{zoom:"100%"}} className="number">{ d3.format("s")(replicationFactor) }</div>
            </div>
          </Visualization>
          <Visualization title="Target Range Size" tooltip="Not yet implemented.">
            <div className="visualization">
              <div style={{zoom:"40%"}} className="number">{ Bytes(targetRangeSize) }</div>
            </div>
          </Visualization>
        </div>
        <pre className="create-table">
          {/* TODO (maxlang): format create table statement */}
          {createTableStatement}
        </pre>
      </div>;
    }
    return <div>No results.</div>;
  }
}

/******************************
 *         SELECTORS
 */

// Helper function that gets a TableDetailsResponseMessage given a state and props
function tableDetails(state: AdminUIState, props: IInjectedProps): TableDetailsResponseMessage {
  let db = props.params[databaseNameAttr];
  let table = props.params[tableNameAttr];
  let details = state.cachedData.tableDetails[generateTableID(db, table)];
  return details && details.data;
}

// Helper function that gets a TableStatsResponseMessage given a state and props
function tableStats(state: AdminUIState, props: IInjectedProps): TableStatsResponseMessage {
  let db = props.params[databaseNameAttr];
  let table = props.params[tableNameAttr];
  let details = state.cachedData.tableStats[generateTableID(db, table)];
  return details && details.data;
}

// Connect the TableMain class with our redux store.
let tableMainConnected = connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      tableDetails: tableDetails(state, ownProps),
      tableStats: tableStats(state, ownProps),
    };
  },
  {
    setUISetting,
    refreshTableDetails,
    refreshTableStats,
  }
)(TableMain);

export default tableMainConnected;
