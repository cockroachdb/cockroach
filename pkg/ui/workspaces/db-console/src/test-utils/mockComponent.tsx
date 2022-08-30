import React from "react";

export function stubComponentInModule(path: string, ...exportedNames: string[]) {
  jest.doMock(path, () => {
    const orig = jest.requireActual(path);

    // An `export = …` module should be replaced with a static render function.
    if (exportedNames.length === 0) {
      console.log("export = (mocked render funtion)");
      return (props: Record<string, unknown>) => <div data-componentname={orig.name} {...props}/> as any;
    }

    // Overwrite exported properties with static render functions.
    const mocks = {...orig};
    if (orig.__esModule === true) {
      mocks.__esModule = true
    }
    for (const name of exportedNames) {
      let candidate: unknown;
      if (typeof orig === "object" && name && orig.hasOwnProperty(name)) {
        console.log(`Found component ${name} in ${path}: ${(orig as any)[name]}`);
        candidate = (orig as any)[name];
      } else {
        throw new Error(`Unable to mock '${path}' property '${name}': property not found`);
      }

      if (typeof candidate === "function") {
        const componentName = name === "default" ? path.split("/").pop() : name;
        mocks[name] = (props: Record<string, unknown>) => <div data-componentname={componentName} {...props}/> as any;
      } else {
        throw new Error(`Unable to mock '${path}' property '${name}', which has type '${typeof candidate}'`);
      }
    }
    return mocks;
  });
}
