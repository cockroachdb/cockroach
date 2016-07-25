import * as React from "react";
import { IndexListLink, ListLink } from "../../components/listLink";
import { IInjectedProps, Link } from "react-router";
import { databaseNameAttr, tableNameAttr } from "../../util/constants";

/**
 * This is the outer component for the databases page.
 */
export default class extends React.Component<IInjectedProps, {}> {
  // TODO(maxlang): truncate long db/table names
  static title(props: IInjectedProps) {
    if (props.params[tableNameAttr]) {
      return <h2><Link to="/databases" >Databases </Link>: <Link to={`/databases/database/${props.params[databaseNameAttr]}`}> {props.params[databaseNameAttr]}</Link>: {props.params[tableNameAttr]}</h2>;
    } else if (props.params[databaseNameAttr]) {
      return <h2><Link to="/databases" >Databases </Link>: {props.params[databaseNameAttr]}</h2>;
    }
    return <h2>Databases</h2>;
  }

  render() {
    let db = this.props.params[databaseNameAttr];
    let table = this.props.params[tableNameAttr];
    let base = "databases" + (db ? `/database/${db}` : "") + (table ? `/table/${table}` : "");

    return <div>
      <div className="nav-container">
        <ul className="nav">
          <IndexListLink to={`${base}/overview`}>Overview</IndexListLink>
          <ListLink to={`${base}/events`}>Events</ListLink>
          {(db || table) ? <ListLink to={`${base}/grants`}>Grants</ListLink> : null}
        </ul>
      </div>
      <div className="section">
        { this.props.children }
      </div></div>;
  }
}
