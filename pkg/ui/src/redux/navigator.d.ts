// Type definitions for react-router-redux 4.0
// Project: https://github.com/rackt/react-router-redux
// Definitions by: Isman Usoh <https://github.com/isman-usoh>,
//                 Noah Shipley <https://github.com/noah79>,
//                 Dimitri Rosenberg <https://github.com/rosendi>,
//                 Karol Janyst <https://github.com/LKay>,
//                 Dovydas Navickas <https://github.com/DovydasNavickas>
// Definitions: https://github.com/DefinitelyTyped/DefinitelyTyped
// TypeScript Version: 2.1

import { Action, Middleware } from "redux";

export function setRouteParam(param: string, value: string): Action;
export const navigatorMiddleware: Middleware;
