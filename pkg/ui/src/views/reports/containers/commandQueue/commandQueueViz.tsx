import _ from "lodash";
import React from "react";
import Long from "long";
import classNames from "classnames";
import * as dagre from "dagre-layout";

import * as protos from "src/js/protos";
import Print from "src/views/reports/containers/range/print";

interface QueueVizProps {
  queue: {
    [ID: string]: protos.cockroach.storage.storagebase.CommandQueuesSnapshot.Command$Properties,
  };
}

interface QueueVizState {
  selectedNodeID: Long;
}

const COMMAND_RADIUS = 10;
const COMMAND_DIAMETER = COMMAND_RADIUS * 2;

export default class CommandQueueViz extends React.Component<QueueVizProps, QueueVizState> {

  constructor() {
    super();
    this.state = {
      selectedNodeID: null,
    };
  }

  computeLayout() {
    const g = new dagre.graphlib.Graph();

    g.setGraph({
      marginx: 20,
      marginy: 20,
      nodesep: 10,
    });
    g.setDefaultEdgeLabel(() => ({}));

    _.forEach(this.props.queue, (command) => {
      g.setNode(command.id.toString(), {
        width: COMMAND_DIAMETER,
        height: COMMAND_DIAMETER,
        command: command,
      });
      command.prereqs.forEach((prereq) => {
        g.setEdge(prereq.toString(), command.id.toString());
      });
    });

    dagre.layout(g);

    return g;
  }

  renderDetailsTable(graph: dagre.graphlib.Graph) {
    if (this.state.selectedNodeID === null) {
      return (<p>Click on a node to see details.</p>);
    }

    const command = graph.node(this.state.selectedNodeID.toString()).command;
    return (
      <table>
        <tbody>
          <tr>
            <th>ID</th>
            <td>{command.id.toString()}</td>
          </tr>
          <tr>
            <th>Read Only</th>
            <td>{(!!command.readonly).toString()}</td>
          </tr>
          <tr>
            <th>Key Range</th>
            <td>{command.key} to {command.end_key}</td>
          </tr>
          <tr>
            <th>Timestamp</th>
            <td>
              <span title="wall">{command.timestamp.wall_time.toString()}</span>
              .
              <span title="logical">{command.timestamp.logical}</span>
              {" "}({Print.Timestamp(command.timestamp)})
            </td>
          </tr>
        </tbody>
      </table>
    );
  }

  nodeIsSelected(nodeID: Long) {
    return this.state.selectedNodeID !== null && this.state.selectedNodeID.equals(nodeID);
  }

  render() {
    const g = this.computeLayout();

    const nodes = g.nodes().map((nodeId) => g.node(nodeId));
    const edges = g.edges().map((edgeId) => g.edge(edgeId));

    if (nodes.length === 0) {
      return (
        <p>No commands in queue</p>
      );
    }

    return (
      <div>
        <svg
          width={g.graph().width}
          height={g.graph().height}
          style={{border: "1px solid black"}}>
          {nodes.map((node) => {
            return (
              <circle
                key={node.command.id.toString()}
                cx={node.x}
                cy={node.y}
                r={COMMAND_RADIUS}
                onClick={() => { this.setState({ selectedNodeID: node.command.id }); }}
                className={classNames(
                  "command-queue__node",
                  node.command.readonly ? "read" : "write",
                  { selected: this.nodeIsSelected(node.command.id) },
                )} />
            );
          })}
          {edges.map((edge: { points: Array<{x: number, y: number}> }, idx: number) => {
            return (
              <polyline
                key={idx}
                points={edge.points.map(({x, y}) => (`${x},${y}`)).join(" ")}
                style={{stroke: "black", fill: "none"}} />
            );
          })}
        </svg>
        {this.renderDetailsTable(g)}
      </div>
    );
  }

}
