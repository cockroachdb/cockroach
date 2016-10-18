import * as React from "react";
import _ from "lodash";
import { connect } from "react-redux";
import classNames from "classnames";

import { AdminUIState } from "../redux/state";
import * as uiData from "../redux/uiData";
import { setUISetting } from "../redux/ui";
import { HELPUS_BANNER_DISMISSED_KEY } from "./banner/helpusBanner";

export interface HelpUsProps {
  optInAttributes: uiData.OptInAttributes;
  saving: boolean;
  loading: boolean;
  saveError: Error;
  loadError: Error;
  helpusDismissed: boolean;
  loadUIData: typeof uiData.loadUIData;
  saveUIData: typeof uiData.saveUIData;
  setUISetting: typeof setUISetting;
}

/**
 * Renders the main content of the help us page.
 */
export class HelpUs extends React.Component<HelpUsProps, uiData.OptInAttributes> {
  state = new uiData.OptInAttributes();

  static title() {
    return <h2>Help Cockroach Labs</h2>;
  }

  componentWillMount() {
    this.props.loadUIData(uiData.KEY_HELPUS);
    if (!this.props.helpusDismissed) {
      this.props.setUISetting(HELPUS_BANNER_DISMISSED_KEY, true);
    }
  }

  componentWillReceiveProps(props: HelpUsProps) {
    if (!props.saveError && !props.loadError) {
      this.setState(props.optInAttributes);
    }
  }

  makeOnChange = (f: (o: uiData.OptInAttributes, newVal: any) => void) => {
    return (e: React.FormEvent) => {
      let target = e.target as HTMLInputElement;
      let value = target.type === "checkbox" ? target.checked : target.value;
      let newState = _.clone(this.state);
      f(newState, value);
      this.setState(newState);
    };
  }

  submit = (e: React.FormEvent) => {
    e.preventDefault();
    let target = e.target as HTMLFormElement;
    // TODO: add "saving..." text and show/hide the required text
    if (target.checkValidity()) {
      this.props.saveUIData(
        { key: uiData.KEY_HELPUS, value: this.state },
        { key: uiData.KEY_OPTIN, value: this.state.optin },
        // Save an additional key to track that this data is not synchronized to
        // the Cockroach Labs server.
        { key: uiData.KEY_REGISTRATION_SYNCHRONIZED, value: false },
      );
    }
    return false;
  }

  render() {
    let attributes: uiData.OptInAttributes = this.state;
    let saving = this.props.saving;
    let {saveError, loadError } = this.props;
    let message = "Saved.";
    if (saving) {
      message = "Saving...";
    } else if (saveError) {
      message = "Save failed";
    }

    let showMessage = saving || saveError;

    if (loadError) {
      return <div className="section">
        <div className="header">Usage Reporting</div>
          <div className="form">
            There was an error retrieving data from CockroachDB. To manually
            delete your opt-in settings, please run the following in the
            CockroachDB sql terminal:
          <pre className="sql">DELETE FROM system.ui WHERE key IN ('helpus', 'registration_synchronized', 'server.optin-reporting');</pre>
          </div>
        </div>;
    }

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
          <input name="email" type="email" required={true} placeholder="Email*" value={attributes.email} onChange={this.makeOnChange((o, v) => o.email = v)} />
          <span className="status"></span>
          <input name="company" placeholder="Company" value={attributes.company} onChange={this.makeOnChange((o, v) => o.company = v)} />
          <span className="status"></span>
          <div>
            <input type="checkbox" name="optin" id="optin" checked={attributes.optin || false} onChange={this.makeOnChange((o, v) => o.optin = v)} />
            <label htmlFor="optin">Share data with Cockroach Labs</label>
            <div className="optin-text">
                  By enabling this feature, you are agreeing to send us anonymous,
                  aggregate information about your running CockroachDB cluster,
                  which may include capacity and usage, server and storage device metadata, basic network topology,
                  and other information that helps us improve our products and services. We never collect any of
                  the actual data that you store in your CockroachDB cluster.
                  Except as set out above, our <a href="/assets/privacyPolicy.html" target="_blank">"Privacy Policy")</a> governs our collection
                  and use of information from users of our products and services.
            </div>
          </div>
          <div>
            {(attributes.updates && (this.props.optInAttributes.updates === this.state.updates)) ? null :
              <div>
                <input type="checkbox" name="updates" id="updates" checked={attributes.updates || false} onChange={this.makeOnChange((o, v) => o.updates = v)} />
                <label htmlFor="updates">Send me product and feature updates.</label>
                <div className="optin-text">
                  You will not be able to deselect this option from the Admin UI.
                </div>
              </div>
            }
          </div>
          <button disabled={saving || !!loadError} className="left">Submit</button>
          <div className={classNames("saving", saving ? "no-animate" : null)} style={showMessage ? { opacity: 1.0 } : null}>{message}</div>
        </form>
      </div>
    </div>;
  }
}

let optinAttributes = (state: AdminUIState): uiData.OptInAttributes => uiData.getData(state, uiData.KEY_HELPUS) || {};
let saving = (state: AdminUIState): boolean => uiData.isSaving(state, uiData.KEY_HELPUS);
let loading = (state: AdminUIState): boolean => uiData.isLoading(state, uiData.KEY_HELPUS);
let saveError = (state: AdminUIState): Error => uiData.getSaveError(state, uiData.KEY_HELPUS);
let loadError = (state: AdminUIState): Error => uiData.getLoadError(state, uiData.KEY_HELPUS);
let helpusDismissed = (state: AdminUIState): boolean => uiData.getData(state, HELPUS_BANNER_DISMISSED_KEY);

// Connect the HelpUs class with our redux store.
let helpusConnected = connect(
  (state: AdminUIState) => {
    return {
      optInAttributes: optinAttributes(state),
      saving: saving(state),
      loading: loading(state),
      saveError: saveError(state),
      loadError: loadError(state),
      helpusDismissed: helpusDismissed(state),
    };
  },
  {
    loadUIData: uiData.loadUIData,
    saveUIData: uiData.saveUIData,
    setUISetting,
  }
)(HelpUs);

export default helpusConnected;
