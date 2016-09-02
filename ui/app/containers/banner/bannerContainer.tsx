import * as React from "react";
import DisconnectedBanner from "./disconnectedBanner";
import HelpusBanner from "./helpusBanner";
import OutdatedBanner from "./outdatedBanner";
import WarningsBanner from "./warningBanner";

/**
 * This is the outer component for all banners
 */
export default class extends React.Component<{}, {}> {
  render() {
    // Note: The order the banners appear here dictates their precedence.
    return <div id="banner">
      <DisconnectedBanner />
      <WarningsBanner />
      <OutdatedBanner />
      <HelpusBanner />
    </div>;
  }
}
