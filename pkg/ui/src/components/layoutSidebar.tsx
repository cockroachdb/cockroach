import * as React from "react";
import { ListLink, LinkProps } from "./listLink";
import * as Icons from "./icons";
import { trustIcon } from "../util/trust";

interface IconLinkProps extends LinkProps {
  icon?: string;
  title?: string;
}

/**
 * IconLink creats a react router Link which contains both a graphical icon and
 * a string title.
 */
class IconLink extends React.Component<IconLinkProps, {}> {
  render() {
    let passProps = {
      to: this.props.to,
      className: this.props.className,
    };
    return <ListLink {...passProps} >
      <div className="image-container"
           dangerouslySetInnerHTML={trustIcon(this.props.icon)}/>
      <div>{this.props.title}</div>
    </ListLink>;
  }
}

/**
 * SideBar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
export default class extends React.Component<{}, {}> {
  render() {
    return <nav className="navigation-bar">
      <ul className="navigation-bar__list">
        <IconLink to="/cluster" icon={Icons.clusterIcon} title="Cluster" />
        <IconLink to="/databases" icon={Icons.databaseIcon} title="Databases"/>
      </ul>
      <ul className="navigation-bar__list navigation-bar__list--bottom">
        <IconLink to="/" icon={Icons.cockroachIcon} className="cockroach" />
      </ul>
    </nav>;
  }
}
