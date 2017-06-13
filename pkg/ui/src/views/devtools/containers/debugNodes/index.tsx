import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { refreshDebugNodes } from "src/redux/apiReducers";
import { RouterState } from "react-router";
import { DebugFailureTable } from "src/views/devtools/components/debugFailureTable";

interface DebugNodesOwnProps {
  debugNodes: protos.cockroach.server.serverpb.DebugNodesResponse;
  refreshDebugNodes: typeof refreshDebugNodes;
}

type DebugNodesProps = DebugNodesOwnProps & RouterState;

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

    const filtersOutput = (filters: string[]) => {
      if (_.isEmpty(filters)) {
        return null;
      }
      return (
        <div>
          <h2>Filters</h2>
          {
            _.map(filters, (filter, i) => (
              <div key={i}>• {filter}</div>
            ))
          }
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
        <table className={tableClass}>
          <tbody>
            {
              _.map(rows, (row, i) => (
                <tr className={rowClassNames(row.classes)} key={i}>
                  {
                    _.map(row.cells, (cell, j) => (
                      <td className={cellClassNames(cell.classes)} key={j} title={cell.title}>
                        {
                          _.map(cell.values, (value, k) => (
                            <span key={k}>
                              {value}
                              <br/>
                            </span>
                          ))
                        }
                      </td>
                    ))
                  }
                </tr>
              ))
            }
          </tbody>
        </table>
      );
    };

    return (
      <div className="section">
        <h1>Node Diagnostics Page</h1>
        <DebugFailureTable failures={debugNodes.failures} />
        {filtersOutput(debugNodes.filters)}
        <h2>Nodes</h2>
        {debugTable(debugNodes.rows, "debugnodes")}
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
