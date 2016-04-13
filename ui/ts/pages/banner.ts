// source: pages/banner.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../components/helpusprompt.ts" />
/// <reference path="../components/outdatedbanner.ts" />
/// <reference path="../components/disconnectedbanner.ts" />

module AdminViews {
  "use strict";

  export module SubModules {
    /**
     * Banner is used for all the different banners
     */
    export module Banner {

      export function controller(): any {}

      export function view(ctrl: any): _mithril.MithrilVirtualElement[] {
        return [
          m.component(Components.DisconnectedBanner),
          m.component(Components.HelpUsPrompt),
          m.component(Components.OutdatedBanner),
        ];
      }
    }
  }
}
