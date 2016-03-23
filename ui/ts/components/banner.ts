// source: component/banner.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

module Components {
  "use strict";

  /**
   * Banner is a component that displays a banner
   */
  export module Banner {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    interface BannerConfig {
      // Class applied to the banner
      bannerClass: string;

      // Banner content
      content: (string|MithrilVirtualElement|MithrilVirtualElement[]|(MithrilVirtualElement|string)[]);

      // Close callback for clicking the X or the screen
      onclose: () => void;
    }

    export function controller(): any {}

    export function view(ctrl: any, bannerConfig: BannerConfig): _mithril.MithrilVirtualElement {
      return m(".banner" + bannerConfig.bannerClass, [
        m(".content", bannerConfig.content),
        m(".close", {onclick: bannerConfig.onclose}, m.trust("&#x2715; ")),
      ]);
    }
  }
}
