/**
 * TODO(mrtracy): During construction of 'next', we are explicitly avoiding any
 * CSS changes from the existing ui. After publishing 'next', we should
 * restructure the css of these links slightly so that the 'active' class is
 * present directly on the <a> element, instead of the containing <li> element;
 * that will allow us to use the react-router's <Link> class to provide the
 * activeClass, rather than the logic below.
 */

import * as React from "react";
import { Link, IRouter } from "react-router";

interface ListLinkContext {
  router: IRouter;
}

export interface LinkProps {
  to: string;
  onlyActiveOnIndex?: boolean;
  className?: string;
}

export class ListLink extends React.Component<LinkProps, {}> {
  static contextTypes = {
    router: React.PropTypes.object,
  };

  static defaultProps = {
    className: "normal",
    onlyActiveOnIndex: false,
  };

  context: ListLinkContext;

  render() {
    let { to, className, onlyActiveOnIndex, children } = this.props;
    if (this.context.router.isActive(to, onlyActiveOnIndex)) {
      if (className) {
        className += " active";
      } else {
        className = "active";
      }
    }
    return <li className={className}>
      <Link to={to}>
        { children }
      </Link>
    </li>;
  }
}

export class IndexListLink extends React.Component<LinkProps, {}> {
  render() {
    return <ListLink {...this.props} onlyActiveOnIndex={true} />;
  }
}
