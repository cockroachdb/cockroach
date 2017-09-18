import React from "react";

import Long from "long";
import * as protos from "src/js/protos";
import * as dagre from "dagre-layout";

interface CommandQueueVizProps {
  // TODO(vilterp): doesn't compile without $Properties; not sure why
  queue: protos.cockroach.storage.CommandQueueSnapshot$Properties;
}

const testData: CommandQueueVizProps = {
  queue: {
    commands: [
      {
        id: Long.fromNumber(0),
        prereqs: [],
        readonly: false,
        span: null,
        timestamp: null,
      },
      {
        id: Long.fromNumber(1),
        prereqs: [
          Long.fromNumber(0),
        ],
        readonly: false,
        span: null,
        timestamp: null,
      },
      {
        id: Long.fromNumber(2),
        prereqs: [
          Long.fromNumber(0),
        ],
        readonly: false,
        span: null,
        timestamp: null,
      },
    ],
  },
};

const VIZ_WIDTH = 500;
const VIZ_HEIGHT = VIZ_WIDTH / 2;

const COMMAND_WIDTH = 60;
const COMMAND_HEIGHT = 40;

export default class CommandQueueViz extends React.Component<CommandQueueVizProps, {}> {

  render() {
    const g = new dagre.graphlib.Graph();

    g.setGraph({});
    g.setDefaultEdgeLabel((_label: string) => ({}));

    testData.queue.commands.forEach((command) => {
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

    const nodeIds = g.nodes() as Array<string>;
    const nodes = nodeIds.map((nodeId) => (
      g.node(nodeId)
    ));
    const edges = g.edges().map((edgeId) => (
      g.edge(edgeId)
    ));
    return (
      <svg width={VIZ_WIDTH} height={VIZ_HEIGHT}>
        {nodes.map((node) => (
          <rect
            key={node.command.id.toString()}
            x={node.x - COMMAND_WIDTH / 2}
            y={node.y - COMMAND_HEIGHT / 2}
            width={node.width}
            height={node.height}
            style={{fill: "lightblue", stroke: "black"}} />
        ))}
        {nodes.map((node) => (
          <text
            key={node.command.id.toString()}
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
    );
  }

}
