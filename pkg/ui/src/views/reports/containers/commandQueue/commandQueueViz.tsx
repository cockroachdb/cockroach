import React from "react";

import * as protos from "src/js/protos";

interface CommandQueueVizProps {
  // TODO(vilterp): doesn't compile without $Properties; not sure why
  queue: protos.cockroach.storage.CommandQueueSnapshot$Properties,
}

export default class CommandQueueViz extends React.Component<CommandQueueVizProps, {}> {

  render() {
    return (
      <pre>
        {JSON.stringify(this.props.queue, null, 2)}
      </pre>
    );
  }

}
