import * as React from "react";
import classNames from "classnames";

interface ToolTipWrapperProps {
  text: React.ReactNode;
}

interface ToolTipWrapperState {
  hovered: boolean;
}

/**
 * ToolTipWrapper wraps its children with an area that detects mouseover events
 * and, when hovered, displays a floating tooltip to the immediate right of
 * the wrapped element.
 *
 * Note that the child element itself must be wrappable; certain CSS attributes
 * such as "float" will render parent elements unable to properly wrap their
 * contents.
 */
export class ToolTipWrapper extends React.Component<ToolTipWrapperProps, ToolTipWrapperState> {
  constructor(props?: ToolTipWrapperProps, context?: any) {
    super(props, context);
    this.state = {
      hovered: false,
    };
  }

  onMouseEnter = () => {
    this.setState({hovered: true});
  }

  onMouseLeave = () => {
    this.setState({hovered: false});
  }

  render() {
    const { text } = this.props;
    const { hovered } = this.state;
    const tooltipClassNames = classNames({
      "hover-tooltip": true,
      "hover-tooltip--hovered": hovered,
    });

    return (
      <div
        className={tooltipClassNames}
        onMouseEnter={this.onMouseEnter}
        onMouseLeave={this.onMouseLeave}
      >
        <div className="hover-tooltip__text">
          { text }
        </div>
        <div className="hover-tooltip__content">
          { this.props.children }
        </div>
      </div>
    );
  }
}
