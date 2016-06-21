import * as React from "react";
import classnames = require("classnames");

interface BannerProps {
  className?: string;
  onclose?: () => void;
  visible?: boolean;
}

/**
 * Banner is a React element that assists in creating banners
 */
export default class extends React.Component<BannerProps, {}> {
  static defaultProps: BannerProps = {
    className: "",
    onclose: () => { },
    visible: false,
  };

  close = () => {
    this.props.onclose();
  }

  render() {
    let visibleClass = this.props.visible ? "expanded" : "hidden";
    return <div className={ classnames("banner", this.props.className, visibleClass)}>
      <div className="content">
        {this.props.children}
      </div>
      <div className="close" onClick={this.close}>
        ✕
      </div>
    </div>;
  }
}
