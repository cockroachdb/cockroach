import * as React from "react";
import _ = require("lodash");
import { connect } from "react-redux";

import { AdminUIState } from "../redux/state";
import { KEY_HELPUS, OptInAttributes, loadUIData, saveUIData } from "../redux/uiData";
import { setUISetting } from "../redux/ui";
import { HELPUS_BANNER_DISMISSED_KEY } from "./banner/helpusBanner";

export interface HelpUsProps {
  optInAttributes: OptInAttributes;
  loadUIData: typeof loadUIData;
  saveUIData: typeof saveUIData;
  setUISetting: typeof setUISetting;
}

/**
 * Renders the main content of the help us page.
 */
export class HelpUs extends React.Component<HelpUsProps, OptInAttributes> {
  state = new OptInAttributes();

  static title() {
    return <h2>Help Cockroach Labs</h2>;
  }

  componentWillMount() {
    this.props.loadUIData(KEY_HELPUS);
    this.props.setUISetting(HELPUS_BANNER_DISMISSED_KEY, true);
  }

  componentWillReceiveProps(props: HelpUsProps) {
    this.setState(props.optInAttributes);
  }

  makeOnChange = (f: (o: OptInAttributes, newVal: any) => void) => {
    return (e: Event) => {
      let target = e.target as HTMLInputElement;
      let value = target.type === "checkbox" ? target.checked : target.value;
      let newState = _.clone(this.state);
      f(newState, value);
      this.setState(newState);
    };
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
        <div className="intro">CockroachDB is in beta, and we're working diligently to make it better.
              Opt in to share basic anonymous usage statistics.
        </div>
        <hr />
        <form onSubmit={this.submit}>
          <input name="firstname" placeholder="First Name" value={attributes.firstname} onChange={this.makeOnChange((o, v) => o.firstname = v)} />
          <span className="status"></span>
          <input name="lastname" placeholder="Last Name" value={attributes.lastname} onChange={this.makeOnChange((o, v) => o.lastname = v)} />
          <span className="status"></span>
          <input name="email" type="email" required="true" placeholder="Email*" value={attributes.email} onChange={this.makeOnChange((o, v) => o.email = v)} />
          <span className="status"></span>
          <input name="company" placeholder="Company" value={attributes.company} onChange={this.makeOnChange((o, v) => o.company = v)} />
          <span className="status"></span>
          <div>
            <input type="checkbox" name="optin" id="optin" checked={attributes.optin} onChange={this.makeOnChange((o, v) => o.optin = v)} />
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
              <input type="checkbox" name="updates" id="updates" checked={attributes.updates} onChange={this.makeOnChange((o, v) => o.updates = v)} />
              <label for="updates">Send me product and feature updates</label>
            </div>
          </div>
          <button className="right">Submit</button>
        </form>
      </div>
    </div>;
  }
}

let optinAttributes = (state: AdminUIState): OptInAttributes => state && state.uiData && state.uiData.data && state.uiData.data[KEY_HELPUS];

// Connect the HelpUs class with our redux store.
let helpusConnected = connect(
  (state: AdminUIState) => {
    return {
      optInAttributes: optinAttributes(state),
    };
  },
  {
    loadUIData,
    saveUIData,
    setUISetting,
  }
)(HelpUs);

export default helpusConnected;
