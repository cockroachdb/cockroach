// source: pages/sql.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/sqlquery.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module AdminViews {
  "use strict";
  export module SQL {
    export module Page {
      import MithrilVirtualElement = _mithril.MithrilVirtualElement;
      import MithrilBasicProperty = _mithril.MithrilBasicProperty;
      import MithrilController = _mithril.MithrilController;
      import MithrilView = _mithril.MithrilView;

      let data: MithrilBasicProperty <any> = m.prop({});

      let query: MithrilBasicProperty <any> = m.prop("");

      function runQuery(q: string): void {
        Models.SQLQuery.runQuery(q).then(data);
      }

      export function controller(): MithrilController {
        return {};
      }

      export let view: MithrilView<any> = function (ctrl: any): MithrilVirtualElement {
        return m(
          "div",
          m("input", {onchange: m.withAttr("value", runQuery), value: query()}),
          m("pre", JSON.stringify(data(), null, 2))
        );
      };
    }
  }
}

