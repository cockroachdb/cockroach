// source: util/types.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
// Author: Max Lang (max@cockroachlabs.com)

import MithrilVirtualElement = _mithril.MithrilVirtualElement;
import MithrilComponent = _mithril.MithrilComponent;

interface Domain {
    toString(): string;
}

type MithrilChild = string | MithrilVirtualElement | MithrilComponent<any> | Array<string | MithrilVirtualElement | MithrilComponent<any>>;
