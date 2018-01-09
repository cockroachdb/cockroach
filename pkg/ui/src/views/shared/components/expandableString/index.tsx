import React from "react";

// ExpandableString displays a short string with a clickable ellipsis. When
// clicked, the component displays long. If short is not specified, it uses
// the first 50 chars of long.

export interface ExpandableStringProps {
  short?: string;
  long: string;
}

export interface ExpandableStringState {
  expanded: boolean;
}

export class ExpandableString extends React.Component<
  ExpandableStringProps,
  ExpandableStringState
> {
  constructor(props?: ExpandableStringProps, context?: any) {
    super(props, context);
    this.state = { expanded: false };
  }
  onClick(ev: any) {
    ev.preventDefault();
    this.setState({ expanded: true });
  }
  render() {
    const short = this.props.short || this.props.long.substr(0, 50).trim();
    if (this.state.expanded || short === this.props.long) {
      return <span>{this.props.long}</span>;
    }
    return (
      <span>
        {short}
        <a href="#" onClick={this.onClick.bind(this)}>
          {" "}
          &hellip;
        </a>
      </span>
    );
  }
}
