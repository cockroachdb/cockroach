// tslint:disable-next-line:no-var-requires
const privacyPolicy = require<string>("../../assets/privacyPolicy.html");

import * as React from "react";
import _ from "lodash";
import { connect } from "react-redux";
import classNames from "classnames";

import { AdminUIState } from "../redux/state";
import * as uiData from "../redux/uiData";
import { helpusBannerDismissedSetting } from "../redux/alerts";

export interface HelpUsProps {
  optInAttributes: uiData.OptInAttributes;
  valid: boolean;
  saving: boolean;
  loading: boolean;
  saveError: Error;
  loadError: Error;
  loadUIData: typeof uiData.loadUIData;
  saveUIData: typeof uiData.saveUIData;
  helpusDismissed: boolean;
  setDismissedBanner: typeof helpusBannerDismissedSetting.set;
}

class HelpUsState {
  optInAttributes = new uiData.OptInAttributes();
  initialized?= false;
  lastSaveFailed?: boolean;
}

/**
 * Renders the main content of the help us page.
 */
export class HelpUs extends React.Component<HelpUsProps, HelpUsState> {
  state = new HelpUsState();

  static title() {
    return <h2>Help Cockroach Labs</h2>;
  }

  componentWillMount() {
    this.setState({ initialized: false });
    this.props.loadUIData(uiData.KEY_HELPUS);
    if (!this.props.helpusDismissed) {
      this.props.setDismissedBanner(true);
    }
  }

  componentWillReceiveProps(props: HelpUsProps) {
    if (props.valid && !this.state.initialized) {
      this.setState({ optInAttributes: props.optInAttributes || new uiData.OptInAttributes(), initialized: true });
    }
    if (props.saveError) {
      this.setState({ lastSaveFailed: true });
    } else if (props.saving) {
      this.setState({ lastSaveFailed: false });
    }
  }

  makeOnChange = (f: (o: uiData.OptInAttributes, newVal: any) => void) => {
    return (e: React.FormEvent<HTMLInputElement>) => {
      const target = e.currentTarget;
      let value = target.type === "checkbox" ? target.checked : target.value;
      let newAttributes = _.clone(this.state.optInAttributes);
      f(newAttributes, value);
      this.setState({optInAttributes: newAttributes});
    };
  }

  submit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const target = e.currentTarget;
    if (target.checkValidity()) {
      this.props.saveUIData(
        { key: uiData.KEY_HELPUS, value: this.state.optInAttributes },
        { key: uiData.KEY_OPTIN, value: this.state.optInAttributes.optin },
        // Save an additional key to track that this data is not synchronized to
        // the Cockroach Labs server.
        { key: uiData.KEY_REGISTRATION_SYNCHRONIZED, value: false },
      );
    }
    return false;
  }

  render() {
    let attributes: uiData.OptInAttributes = this.state.optInAttributes;
    let saving = this.props.saving;
    let {saveError, loadError } = this.props;
    let message = "Saved.";
    if (saving) {
      message = "Saving...";
    } else if (this.state.lastSaveFailed) {
      message = "Save failed";
    }

    let showMessage = saving || saveError || this.state.lastSaveFailed;

    if (!this.state.initialized) {
      return <div className="section">
        <div className="header">Usage Reporting</div>
          <div className="form">
            {loadError ? "There was an error retrieving data from CockroachDB." : "Loading optin data."} To manually
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
          <input name="firstname" placeholder="First Name" value={attributes.firstname || ""} onChange={this.makeOnChange((o, v) => o.firstname = v)} />
          <span className="status"></span>
          <input name="lastname" placeholder="Last Name" value={attributes.lastname || ""} onChange={this.makeOnChange((o, v) => o.lastname = v)} />
          <span className="status"></span>
          <input name="email" type="email" required={true} placeholder="Email*" value={attributes.email || ""} onChange={this.makeOnChange((o, v) => o.email = v)} />
          <span className="status"></span>
          <input name="company" placeholder="Company" value={attributes.company || ""} onChange={this.makeOnChange((o, v) => o.company = v)} />
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
                  Except as set out above, our <a href={privacyPolicy} target="_blank">"Privacy Policy"</a> governs our collection
                  and use of information from users of our products and services.
            </div>
          </div>
          <div>
            {(attributes.updates && (this.props.optInAttributes.updates === this.state.optInAttributes.updates)) ? null :
              <div>
                <input type="checkbox" name="updates" id="updates" checked={attributes.updates || false} onChange={this.makeOnChange((o, v) => o.updates = v)} />
                <label htmlFor="updates">Send me product and feature updates.</label>
                <div className="optin-text">
                  You will not be able to deselect this option from the Admin UI.
                </div>
              </div>
            }
          </div>
          <button disabled={saving} className="left">Submit</button>
          <div className={classNames("saving", saving ? "no-animate" : null)} style={showMessage ? { opacity: 1.0 } : null}>{message}</div>
        </form>
      </div>
    </div>;
  }
}

// Get the current data even if it isn't valid.
let optinAttributes = (state: AdminUIState): uiData.OptInAttributes => state.uiData[uiData.KEY_HELPUS].data;
let valid = (state: AdminUIState): boolean => uiData.isValid(state, uiData.KEY_HELPUS);
let saving = (state: AdminUIState): boolean => uiData.isSaving(state, uiData.KEY_HELPUS);
let loading = (state: AdminUIState): boolean => uiData.isLoading(state, uiData.KEY_HELPUS);
let saveError = (state: AdminUIState): Error => uiData.getSaveError(state, uiData.KEY_HELPUS);
let loadError = (state: AdminUIState): Error => uiData.getLoadError(state, uiData.KEY_HELPUS);

// Connect the HelpUs class with our redux store.
let helpusConnected = connect(
  (state: AdminUIState) => {
    return {
      optInAttributes: optinAttributes(state),
      valid: valid(state),
      saving: saving(state),
      loading: loading(state),
      saveError: saveError(state),
      loadError: loadError(state),
      helpusDismissed: helpusBannerDismissedSetting.selector(state),
    };
  },
  {
    loadUIData: uiData.loadUIData,
    saveUIData: uiData.saveUIData,
    setDismissedBanner: helpusBannerDismissedSetting.set,
  },
)(HelpUs);

export default helpusConnected;
