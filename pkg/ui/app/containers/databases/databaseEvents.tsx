import * as React from "react";
import Events from "../events";

export default class extends React.Component<{}, {}> {
  render() {
    return <div className="database-events table">
      <Events />
    </div>;
  }
}
