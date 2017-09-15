import React from "react";

import Long from 'long';
import * as protos from "src/js/protos";
import * as cola from "webcola";

interface CommandQueueVizProps {
  // TODO(vilterp): doesn't compile without $Properties; not sure why
  queue: protos.cockroach.storage.CommandQueueSnapshot$Properties,
}

interface CommandQueueVizState {
  layout: cola.Layout,
}

interface CommandNode extends cola.InputNode {
  command: protos.cockroach.storage.CommandQueueCommand$Properties,
}

const testData: CommandQueueVizProps = {
  queue: {
    commands: [
      {
        id: Long.fromNumber(0),
        prereqs: [],
        readonly: false,
        span: null,
        timestamp: null
      },
      {
        id: Long.fromNumber(1),
        prereqs: [
          Long.fromNumber(0)
        ],
        readonly: false,
        span: null,
        timestamp: null
      }
    ]
  }
};

const VIZ_WIDTH = 500;
const VIZ_HEIGHT = 500;

const COMMAND_WIDTH = 60;
const COMMAND_HEIGHT = 40;

export default class CommandQueueViz extends React.Component<CommandQueueVizProps, CommandQueueVizState> {

  constructor() {
    super();
    this.state = {
      layout: new cola.Layout(),
    };
  }

  /**
   * Load data into layout, start layout iteration, and trigger rendering when layout is done.
   */
  setupLayout() {
    console.log('QUEUE', this.props.queue.commands);
    const layout = this.state.layout;
    
    const nodes = this.props.queue.commands.map((command) => ({
      command,
      width: COMMAND_WIDTH,
      height: COMMAND_HEIGHT,
    }));
    layout.nodes(nodes);

    const nodesById = {} as { [key: string]: CommandNode };
    nodes.forEach((node: CommandNode) => {
      nodesById[node.command.id.toString()] = node;
    });

    const links = [] as Array<cola.Link<CommandNode>>;
    this.props.queue.commands.forEach((command) => {
      command.prereqs.forEach((prereq) => {
        links.push({
          source: nodesById[prereq.toString()],
          target: nodesById[command.id.toString()],
        });
      });
    });
    layout.links(links);

    // TODO(vilterp): generate constraints that put boxes next to each other

    layout.on("end", (_evt) => {
      // this doesn't actually change the state since it points to the same object
      // really just want to trigger a re-render without copying anything
      this.setState({
        layout
      });
    });

    layout.flowLayout("y", COMMAND_HEIGHT + 20);
    layout.size([VIZ_WIDTH, VIZ_HEIGHT]);

    layout.start(10, 10, 10);
  }
  
  componentDidMount() {
    this.setupLayout();
  }

  render() {
    const nodes = this.state.layout.nodes() as Array<CommandNode>;
    const links = this.state.layout.links() as Array<cola.Link<CommandNode>>;
    console.log('links', links);
    return (
      <svg width={VIZ_WIDTH} height={VIZ_HEIGHT}>
        {nodes.map((node) => (
          <rect
            key={node.command.id.toString()}
            x={node.x}
            y={node.y}
            width={node.width}
            height={node.height}
            style={{fill: "lightblue", stroke: "black"}} />
        ))}
        {nodes.map((node) => (
          <text
            key={node.command.id.toString()}
            x={node.x + COMMAND_WIDTH/2}
            y={node.y + COMMAND_HEIGHT/2}>
            {node.command.id.toString()}
          </text>
        ))}
        {links.map((link) => (
          <line
            key={`${link.source.command.id.toString()}-${link.target.command.id.toString()}`}
            x1={link.source.x + COMMAND_WIDTH/2}
            y1={link.source.y + COMMAND_HEIGHT}
            x2={link.target.x + COMMAND_WIDTH/2}
            y2={link.target.y}
            // TODO: line ending: arrow (copy from DistSQL plan viz)
            style={{stroke: "black"}} />
        ))}
      </svg>
    );
  }

}
