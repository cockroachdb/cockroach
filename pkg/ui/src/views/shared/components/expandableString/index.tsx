import React from "react";

import { trustIcon } from "src/util/trust";

import expandIcon from "!!raw-loader!assets/expand.svg";

import "./expandable.styl";

interface ExpandableStringProps {
  short?: string;
  long: string;
}

interface ExpandableStringState {
  expanded: boolean;
}

// ExpandableString displays a short string with a clickable ellipsis. When
// clicked, the component displays long. If short is not specified, it uses
// the first 50 chars of long.
export class ExpandableString extends React.Component<ExpandableStringProps, ExpandableStringState> {
  state: ExpandableStringState = {
    expanded: false,
  };
  onClick = (ev: any) => {
    ev.preventDefault();
    this.setState({ expanded: true });
  }
  render() {
    const truncateLength = 50;
    if (this.state.expanded || this.props.long.length <= truncateLength + 2) {
      return <span>{this.props.long}</span>;
    }
    const short = this.props.short || this.props.long.substr(0, truncateLength).trim();
    return (
      <div className="expandable" onClick={this.onClick}>
        <span className="expandable__text">{ short }&nbsp;&hellip;</span>
        <span className="expandable__icon" dangerouslySetInnerHTML={ trustIcon(expandIcon) } />
      </div>
    );
  }
}
