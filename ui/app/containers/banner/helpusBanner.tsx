import * as React from "react";
import { connect } from "react-redux";
import { Link } from "react-router";
import { createSelector } from "reselect";
import _ = require("lodash");

import Banner from "./banner";
import { AdminUIState } from "../../redux/state";
import { KEY_HELPUS, OptInAttributes, loadUIData, saveUIData } from "../../redux/uiData";
import { setUISetting } from "../../redux/ui";

export let HELPUS_BANNER_DISMISSED_KEY = "banner/helpusBanner/DISMISSED";

class HelpusBannerProps {
  attributes: OptInAttributes;
  attributesLoaded: boolean;
  dismissed: boolean = false;
  loadUIData: typeof loadUIData;
  saveUIData: typeof saveUIData;
  setUISetting: typeof setUISetting;
}

class HelpusBanner extends React.Component<HelpusBannerProps, {}> {
  componentWillMount() {
    this.props.loadUIData(KEY_HELPUS);
  }

  dismiss = () => {
    setUISetting(HELPUS_BANNER_DISMISSED_KEY, true);
    if (this.props.attributesLoaded) {
      let attributes = this.props.attributes || new OptInAttributes();
      attributes.dismissed = attributes.dismissed ? attributes.dismissed + 1 : 1;
      this.props.saveUIData({
        key: "helpus",
        value: attributes,
      });
    }
  }

  render() {
    /** The banner is only visible when:
     *   - the banner has not been dismissed in the current UI session AND
     *   - the optin attributes have loaded AND
     *   - either the optin attributes don't exist OR
     *   - the optin attributes do exist, but the user hasn't dismissed or opted in or out
     */
    let visible: boolean = this.props.attributesLoaded &&
      !this.props.dismissed &&
      (!this.props.attributes ||
        (!this.props.attributes.dismissed && !_.isBoolean(this.props.attributes.optin)));

    return <Banner className="helpus" visible={visible} onclose={this.dismiss}>
      <span className="checkmark">✓ </span>
      Help us improve. Opt in to share usage reporting statistics.
      <Link to="/help-us/reporting"><button onClick={this.dismiss}>Opt In</button></Link>
    </Banner>;
  }
}

interface UIDataState {
  data: { [key: string]: any; };
}

let uiDataState = (state: AdminUIState): UIDataState => state && state.uiData;

// optinAttributes are the saved attributes that indicate whether the user has
// opted in to usage reporting
let optinAttributes = createSelector(
  uiDataState,
  (state: UIDataState) => state && state.data[KEY_HELPUS]
);

// attributesLoaded is a boolean that indicates whether the optinAttributes have been loaded yet
let attributesLoaded = createSelector(
  uiDataState,
  (state: UIDataState) => state && _.has(state.data, KEY_HELPUS)
);

let helpusBannerDismissed = (state: AdminUIState): boolean => state.ui[HELPUS_BANNER_DISMISSED_KEY] || false;

// Connect the HelpUsBanner class with our redux store.
let helpusBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      attributes: optinAttributes(state),
      attributesLoaded: attributesLoaded(state),
      dismissed: helpusBannerDismissed(state),
    };
  },
  {
    loadUIData,
    saveUIData,
    setUISetting,
  }
)(HelpusBanner);

export default helpusBannerConnected;
