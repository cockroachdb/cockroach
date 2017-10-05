// This file imports polyfills necessary for both the unit tests and the
// deployed bundle.
//
// If you're tempted to instead import babel-polyfill, which bundles the most
// common polyfills, fair warning: babel-polyfill, jsdom, and Node 6 are
// mutually incompatible.
//
// TODO(benesch): reevaluate babel-polyfill when we upgrade to Node 8.

import "regenerator-runtime/runtime";
