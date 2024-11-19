// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Only export functions here which are used in both db-console
// and cluster-ui. The purpose of these functions should be to
// reduce the amount of repeated logic in selector bodies throughout
// cluster-ui and db-console.
//
// Example:
// Suppose we have an api response, responseData.
// In both cluster-ui and db-console, responseData is stored in redux.
// We would like to convert responseData -> componentData, and so we
// create two distinct selectors in cluster-ui and db-console that both look
// something like this:
//
// const selector = createSelector(
//     functionToGetApiResponseFromReduxStore,
//     ...otherSelectors,
//     (apiResponse, ...otherStuff) => {
//         /* some logic that converts apiResponse -> componentData format */
//     }
// );
//
// We need these distinct selectors because the shape of redux is different
// in both packages. For some cases, the logic in the combiner function
// body to convert the redux data to the expected format can be quite
// lengthy (e.g. statement sql stats) and when we make changes / fixes to them
// we need to change both selector functions (essentially maintaining a copy in
// each repo).
// To reduce this, we can use the same combiner functions so long as we are able
// to provide the same parameters to the function.  Thus, we can export something
// like the below in the cluster-ui package to reuse across both:
//
// Exported function:
// export const combinerFunc = (resultA, resultB) => {
//     /* convert to expected format */
// }
//
// Usage:
// const mySelector = createSelector(
//     selectA,
//     selectB,
//     combinerFunc, <-- all the important logic
// )

export * from "./common";
export * from "./activeExecutionsCommon.selectors";
export * from "./insightsCommon.selectors";
