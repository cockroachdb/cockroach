/// <reference path="../../typings/index.d.ts" />
import * as React from "react";
import { connect } from "react-redux";
import { KEY_HELPUS, OptInAttributes, KeyValue, loadUIData, saveUIData } from "../redux/uiData";

export interface HelpUsProps {
  optInAttributes: OptInAttributes;
  loadUIData: (...keys: string[]) => void;
  saveUIData: (...values: KeyValue[]) => void;
}

/**
 * Renders the main content of the help us page.
 */
export class HelpUs extends React.Component<HelpUsProps, OptInAttributes> {
  static title() {
    return <h2>Help Cockroach Labs</h2>;
  }

  constructor(props: any) {
    super(props);
    this.state = new OptInAttributes();
  }

  componentWillMount() {
    this.props.loadUIData(KEY_HELPUS);
  }

  componentWillReceiveProps = (props: HelpUsProps) => {
    this.setState(props.optInAttributes);
  }

  onChange = (e: Event) => {
    let target = e.target as HTMLInputElement;
    this.setState({ [target.name]: target.type === "checkbox" ? target.checked : target.value } as any);
  }

  submit = (e: Event) => {
    e.preventDefault();
    let target = e.target as HTMLFormElement;
    // TODO: add "saving..." text and show/hide the required text
    if (target.checkValidity()) {
      this.props.saveUIData({ key: KEY_HELPUS, value: this.state });
    }
    return false;
  }

  render() {
    let attributes: OptInAttributes = this.state;
    return <div className="section">
      <div className="header">Usage Reporting</div>
      <div className="form">
        <div classname="intro">CockroachDB is in beta, and we're working diligently to make it better.
              Opt in to share basic anonymous usage statistics.
        </div>
        <hr />
        <form onSubmit={this.submit}>
          <input name="firstname" placeholder="First Name" value={attributes.firstname} onChange={this.onChange} />
          <span className="status"></span>
          <input name="lastname" placeholder="Last Name" value={attributes.lastname} onChange={this.onChange} />
          <span className="status"></span>
          <input name="email" type="email" required="true" placeholder="Email*" value={attributes.email} onChange={this.onChange} />
          <span className="status"></span>
          <input name="company" placeholder="Company" value={attributes.company} onChange={this.onChange} />
          <span className="status"></span>
          <div>
            <input type="checkbox" name="optin" id="optin" checked={attributes.optin} onChange={this.onChange} />
            <label for="optin">Share data with Cockroach Labs</label>
            <div className="optin-text">
                  By enabling this feature, you are agreeing to send us anonymous,
                  aggregate information about your running CockroachDB cluster,
                  which may include capacity and usage, server and storage device metadata, basic network topology,
                  and other information that helps us improve our products and services. We never collect any of
                  the actual data that you store in your CockroachDB cluster.
                  Except as set out above, our <a href="/assets/privacyPolicy.html target="_blank>"Privacy Policy")</a> governs our collection
                  and use of information from users of our products and services.
            </div>
          </div>
          <div>
            <div>
              <input type="checkbox" name="updates" id="updates" checked={attributes.updates} onChange={this.onChange} />
              <label for="updates">Send me product and feature updates</label>
            </div>
          </div>
          <button className="right">Submit</button>
        </form>
      </div>
    </div>;
  }
}

let optinAttributes = (state: any): OptInAttributes => state && state.uiData && state.uiData.data && state.uiData.data[KEY_HELPUS];

// Connect the EventsList class with our redux store.
let eventsConnected = connect(
  (state, ownProps) => {
    return {
      optInAttributes: optinAttributes(state),
    };
  },
  {
    loadUIData,
    saveUIData,
  }
)(HelpUs);

export default eventsConnected;
