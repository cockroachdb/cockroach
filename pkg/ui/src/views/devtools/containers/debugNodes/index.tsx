import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { refreshDebugNodes } from "src/redux/apiReducers";
import { RouterState } from "react-router";

interface DebugNodesOwnProps {
  debugNodes: protos.cockroach.server.serverpb.DebugNodesResponse;
  refreshDebugNodes: typeof refreshDebugNodes;
}

type DebugNodesProps = DebugNodesOwnProps & RouterState;

const failuresTable = (failures: protos.cockroach.server.serverpb.DebugFailure$Properties[]) => {
  if (_.isEmpty(failures)) {
    return null;
  }
  return (
    <div>
      <h2>Failures</h2>
      <div className="failure-table">
        <div className="failure-table__row failure-table__row--header">
          <div className="failure-table__cell failure-table__cell--short">Node</div>
          <div className="failure-table__cell">Error</div>
        </div>
        {
          _.map(failures, (failure) => (
            <div className="failure-table__row" key={failure.node_id}>
              <div className="failure-table__cell failure-table__cell--short">n{failure.node_id}</div>
              <div className="failure-table__cell">title={failure.error_message}>{failure.error_message}</div>
            </div>
          ))
        }
      </div>
    </div>
  );
};

const filtersOutput = (filters: string[]) => {
  if (_.isEmpty(filters)) {
    return null;
  }
  return (
    <div>
      <h2>Filters</h2>
      <ul className="debug-filter-ul">
        {
          _.map(filters, (filter, i) => (
            <li key={i}>{filter}</li>
          ))
        }
      </ul>
    </div>
  );
};

const debugTable = (rows: protos.cockroach.server.serverpb.DebugRow$Properties[], style: string) => {
  if (_.isEmpty(rows)) {
    return null;
  }
  if (rows.length === 0 && _.isEmpty(rows[0].cells)) {
    return null;
  }
  const tableClass = style + "-table";
  const rowClass = tableClass + "__row";
  const cellClass = tableClass + "__cell";
  const rowClassNames = (classNames: string[]) =>
    _.join([rowClass, ..._.map(classNames, (className) => {
      return rowClass + "--" + className;
    })], " ");
  const cellClassNames = (classNames: string[]) =>
    _.join([cellClass, ..._.map(classNames, (className) => {
      return cellClass + "--" + className;
    })], " ");
  return (
    <div className={tableClass}>
      {
        _.map(rows, (row, i) => (
          <div className={rowClassNames(row.classes)} key={i}>
            {
              _.map(row.cells, (cell, j) => (
                <div className={cellClassNames(cell.classes)} key={j} title={cell.title}>
                  <ul className="debug-empty-ul">
                    {
                      _.map(cell.values, (value, k) => (
                        <li key={k}>{value}</li>
                      ))
                    }
                  </ul>
                </div>
              ))
            }
          </div>
        ))
      }
    </div>
  );
};

/**
 * Renders the Debug Nodes page.
 */
class DebugNodes extends React.Component<DebugNodesProps, {}> {
  refresh(props = this.props) {
    props.refreshDebugNodes(new protos.cockroach.server.serverpb.DebugNodesRequest({
      node_ids: (!_.isEmpty(props.location.query.node_ids)) ? props.location.query.node_ids : "",
      locality: (!_.isEmpty(props.location.query.locality)) ? props.location.query.locality : "",
    }));
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(nextProps: DebugNodesProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  render() {
    const { debugNodes } = this.props;
    if (!debugNodes) {
      return (
        <div className="section">
          <h1>Loading cluster status...</h1>
        </div>
      );
    }

    return (
      <div className="section">
        <h1>Node Diagnostics Page</h1>
        {failuresTable(debugNodes.failures)}
        {filtersOutput(debugNodes.filters)}
        {(!_.isEmpty(debugNodes.rows) && debugNodes.rows[0].cells.length > 1) ?
          <div>
            <h2>Nodes</h2>
            {debugTable(debugNodes.rows, "debugnodes")}
          </div> :
          <h2> No results to display. </h2>
        }
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      debugNodes: state.cachedData.debugNodes.data,
    };
  },
  {
    refreshDebugNodes,
  },
)(DebugNodes);
