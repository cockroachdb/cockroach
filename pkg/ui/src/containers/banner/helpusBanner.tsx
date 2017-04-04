import * as React from "react";
import { connect } from "react-redux";
import { Link } from "react-router";
import { createSelector } from "reselect";
import _ from "lodash";

import Banner from "./banner";
import { AdminUIState } from "../../redux/state";
import { KEY_HELPUS, OptInAttributes, loadUIData, saveUIData, UIDataSet } from "../../redux/uiData";
import { LocalSetting } from "../../redux/localsettings";

export const bannerDismissedSetting = new LocalSetting(
  "banner/helpusBanner/DISMISSED", (s: AdminUIState) => s.ui, false,
);

class HelpusBannerProps {
  attributes: OptInAttributes;
  attributesLoaded: boolean;
  dismissed: boolean;
  loadUIData: typeof loadUIData;
  saveUIData: typeof saveUIData;
  setDismissed: typeof bannerDismissedSetting.set;
}

class HelpusBanner extends React.Component<HelpusBannerProps, {}> {
  componentWillMount() {
    this.props.loadUIData(KEY_HELPUS);
  }

  dismiss = () => {
    this.props.setDismissed(true);
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

let uiDataState = (state: AdminUIState): UIDataSet => state.uiData;

// optinAttributes are the saved attributes that indicate whether the user has
// opted in to usage reporting
let optinAttributes = createSelector(
  uiDataState,
  (state: UIDataSet) => state[KEY_HELPUS] && state[KEY_HELPUS].data,
);

// attributesLoaded is a boolean that indicates whether the optinAttributes have been loaded yet
let attributesLoaded = createSelector(
  uiDataState,
  (state: UIDataSet) => state && _.has(state, KEY_HELPUS),
);

// Connect the HelpUsBanner class with our redux store.
let helpusBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      attributes: optinAttributes(state),
      attributesLoaded: attributesLoaded(state),
      dismissed: bannerDismissedSetting.selector(state),
    };
  },
  {
    loadUIData,
    saveUIData,
    setDismissed: bannerDismissedSetting.set,
  },
)(HelpusBanner);

export default helpusBannerConnected;
