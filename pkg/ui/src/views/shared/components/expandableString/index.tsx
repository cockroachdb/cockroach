import React from "react";

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
      <span>
        {short}
        <a href="#" onClick={this.onClick}>
          &hellip;
        </a>
      </span>
    );
  }
}
