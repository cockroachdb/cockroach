// Compiled using typings@0.6.6
// Source: https://raw.githubusercontent.com/DefinitelyTyped/DefinitelyTyped/3b7f250dcf631f1c3c545e28d9ff25920c60b5f7/assertion-error/assertion-error.d.ts
// Type definitions for assertion-error 1.0.0
// Project: https://github.com/chaijs/assertion-error
// Definitions by: Bart van der Schoor <https://github.com/Bartvds>
// Definitions: https://github.com/borisyankov/DefinitelyTyped

declare module 'assertion-error' {
	class AssertionError implements Error {
		constructor(message: string, props?: any, ssf?: Function);
		name: string;
		message: string;
		showDiff: boolean;
		stack: string;
	}
	export = AssertionError;
}