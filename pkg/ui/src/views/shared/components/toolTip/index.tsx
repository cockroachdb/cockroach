import React from "react";
import classNames from "classnames";
import Popper from "popper.js";

import "./tooltip.styl";

interface ToolTipWrapperProps {
  text: React.ReactNode;
  short?: boolean;
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
  popperInstance: Popper;
  content: React.RefObject<HTMLDivElement> = React.createRef();
  text: React.RefObject<HTMLDivElement> = React.createRef();

  constructor(props?: ToolTipWrapperProps, context?: any) {
    super(props, context);
    this.state = {
      hovered: false,
    };
  }

  componentWillUnmount() {
    if (this.popperInstance) {
      this.popperInstance.destroy();
    }
  }

  initPopper() {
    // PopperOptions.eventsEnabled should be set to `false` to prevent
    // performance issues on pages with a large number of tooltips
    this.popperInstance = new Popper(this.content.current, this.text.current, {
      placement: "auto",
      eventsEnabled: false,
    });
  }

  onMouseEnter = () => {
    this.setState({hovered: true});
    this.initPopper();
  }

  onMouseLeave = () => {
    this.setState({hovered: false});
  }

  render() {
    const { text, short } = this.props;
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
        <div className="hover-tooltip__content" ref={this.content}>
          { this.props.children }
        </div>
        <div className="hover-tooltip__text" ref={this.text}>
          { text }
        </div>
      </div>
    );
  }
}
