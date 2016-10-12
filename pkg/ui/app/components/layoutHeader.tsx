import * as React from "react";

/**
 * TopBar represents the static header bar that is present on all pages. The
 * primary "title" element does differ from page to page, and should be passed
 * into TopBar as its child element.
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <div className="topbar">
      {this.props.children}
    </div>;
  }
}
