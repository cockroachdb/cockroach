import React from "react";

import Long from "long";
import * as protos from "src/js/protos";
import * as dagre from "dagre-layout";

interface QueueVizProps {
  // TODO(vilterp): doesn't compile without $Properties; not sure why
  queue: protos.cockroach.storage.CommandQueueSnapshot$Properties;
}

interface QueueVizState {
  hoveredNode: Long;
}

const COMMAND_WIDTH = 60;
const COMMAND_HEIGHT = 40;

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
    });
    g.setDefaultEdgeLabel((_label: string) => ({}));

    this.props.queue.commands.forEach((command) => {
      g.setNode(command.id.toString(), {
        width: COMMAND_WIDTH,
        height: COMMAND_HEIGHT,
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
              {command.timestamp.logical} Logical{" "}
              {command.timestamp.wall_time.toString()} Wall
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
          {nodes.map((node) => (
            <rect
              key={node.command.id.toString()}
              x={node.x - COMMAND_WIDTH / 2}
              y={node.y - COMMAND_HEIGHT / 2}
              onMouseEnter={() => { this.setState({ hoveredNode: node.command.id }); }}
              width={node.width}
              height={node.height}
              style={{
                fill: node.command.readonly ? "lightgreen" : "pink",
                stroke: (this.state.hoveredNode !== null && node.command.id.equals(this.state.hoveredNode))
                  ? "red" : "black",
              }} />
          ))}
          {nodes.map((node) => (
            <text
              key={node.command.id.toString()}
              textAnchor="middle"
              dominantBaseline="middle"
              x={node.x}
              y={node.y}>
              {node.command.id.toString()}
            </text>
          ))}
          {edges.map((edge, idx: number) => {
            return (
              <polyline
                key={idx}
                points={edge.points.map(({x, y}) => (`${x},${y}`)).join(" ")}
                // TODO: line ending: arrow (copy from DistSQL plan viz)
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
