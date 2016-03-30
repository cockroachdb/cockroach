// source: pages/footer.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

module AdminViews {
  "use strict";

  export module SubModules {
    /**
     * Banner is used for all the different banners
     */
    export module Footer {

      export function controller(): any {}

      export function view(ctrl: any): _mithril.MithrilVirtualElement {
        return m(".footer", m(".links", [
          m("a", {href: "https://gitter.im/cockroachdb/cockroach"}, "Gitter"),
          m("a", {href: "https://groups.google.com/forum/#!forum/cockroachdb-users"}, "Google Groups"),
          m("a", {href: "https://twitter.com/cockroachdb"}, "Twitter"),
          m("a", {href: "https://github.com/cockroachdb/cockroach"}, "Github"),
          m("a", {href: "https://www.cockroachlabs.com/"}, "Homepage"),
          m("a", {href: "https://www.cockroachlabs.com/docs/"}, "Docs"),
        ]));
      }
    }
  }
}
