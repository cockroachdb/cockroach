import React from "react";
import { Link } from "react-router";
import * as Icons from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";

interface IconLinkProps {
  icon?: string;
  title?: string;
  to: string;
  onlyActiveOnIndex?: boolean;
  className?: string;
}

/**
 * IconLink creats a react router Link which contains both a graphical icon and
 * a string title.
 */
class IconLink extends React.Component<IconLinkProps, {}> {
  static defaultProps = {
    className: "normal",
    onlyActiveOnIndex: false,
  };

  render() {
    const { icon, title, to, onlyActiveOnIndex, className } = this.props;
    return (
      <li className={className} >
        <Link to={to} activeClassName="active" onlyActiveOnIndex={onlyActiveOnIndex}>
          <div className="image-container"
               dangerouslySetInnerHTML={trustIcon(icon)}/>
          <div>{title}</div>
        </Link>
      </li>
    );
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
        <IconLink to="/overview" icon={Icons.nodesIcon} title="Overview" />
        <IconLink to="/cluster" icon={Icons.clusterIcon} title="Cluster" />
        <IconLink to="/databases" icon={Icons.databaseIcon} title="Databases"/>
        <IconLink to="/jobs" icon={Icons.jobsIcon} title="Jobs"/>
      </ul>
      <ul className="navigation-bar__list navigation-bar__list--bottom">
        <IconLink to="/" icon={Icons.cockroachIcon} className="cockroach" />
      </ul>
    </nav>;
  }
}
