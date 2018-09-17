import React from "react";
import classNames from "classnames";

import "./tooltip.styl";

interface ToolTipWrapperProps {
  text: React.ReactNode;
  short?: boolean;
  placement?: "top" | "right" | "bottom" | "left";
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
  static defaultProps = {
    placement: "bottom",
  };

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
    const { text, short, placement } = this.props;
    const { hovered } = this.state;
    const tooltipClassNames = classNames({
      "hover-tooltip": true,
      "hover-tooltip--hovered": hovered,
      "hover-tooltip--short": short,
    });

    return (
      <div
        className={tooltipClassNames}
        onMouseEnter={this.onMouseEnter}
        onMouseLeave={this.onMouseLeave}
      >
        <div className={"hover-tooltip__text " + placement}>
          { text }
        </div>
        <div className="hover-tooltip__content">
          { this.props.children }
        </div>
      </div>
    );
  }
}
