import React from "react";

import Long from "long";
import Print from "src/views/reports/containers/range/print";
import * as dagre from "dagre";

import * as protos from "src/js/protos";

interface QueueVizProps {
  queue: protos.cockroach.storage.CommandQueueSnapshot$Properties;
}

interface QueueVizState {
  hoveredNode: Long;
}

const COMMAND_RADIUS = 10;
const COMMAND_DIAMETER = COMMAND_RADIUS * 2;

export default class CommandQueueViz extends React.Component<QueueVizProps, QueueVizState> {

  constructor() {
    super();
    this.state = {
      hoveredNode: null,
    };
  }

  computeLayout() {
    const g = new dagre.graphlib.Graph();

    g.setGraph({
      marginx: 20,
      marginy: 20,
      nodesep: 10,
    });

    this.props.queue.commands.forEach((command) => {
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

  detailsTable(command: protos.cockroach.storage.CommandQueueCommand$Properties) {
    return (
      <table>
        <tbody>
          <tr>
            <td>ID</td>
            <td>{`${command.id.toString()}`}</td>
          </tr>
          <tr>
            <td>Read Only</td>
            <td>{`${command.readonly}`}</td>
          </tr>
          <tr>
            <td>Key Range</td>
            <td>{command.key} to {command.end_key}</td>
          </tr>
          <tr>
            <td>Timestamp</td>
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

  render() {
    const g = this.computeLayout();

    const nodeIds = g.nodes() as Array<string>;
    const nodes = nodeIds.map((nodeId) => (
      g.node(nodeId)
    ));
    const edges = g.edges().map((edgeId) => (
      g.edge(edgeId)
    ));

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
            if (!node.command) {
              console.log("no command", node);
            }
            return (
              <circle
                key={node.command.id.toString()}
                cx={node.x}
                cy={node.y}
                r={COMMAND_RADIUS}
                onClick={() => { this.setState({ hoveredNode: node.command.id }); }}
                style={{
                  fill: node.command.readonly ? "lightgreen" : "pink",
                  stroke: (this.state.hoveredNode !== null && node.command.id.equals(this.state.hoveredNode))
                    ? "red" : "black",
                  cursor: "pointer",
                }} />
            );
          })}
          {edges.map((edge, idx: number) => {
            return (
              <polyline
                key={idx}
                points={
                  (edge.points as Array<{x: number, y: number}>)
                    .map(({x, y}) => (`${x},${y}`)).join(" ")
                }
                style={{stroke: "black", fill: "none"}} />
            );
          })}
        </svg>
        {this.state.hoveredNode !== null
          ? this.detailsTable(g.node(this.state.hoveredNode.toString()).command)
          : null}
      </div>
    );
  }

}
