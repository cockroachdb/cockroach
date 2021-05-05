- Feature Name: Cluster UI Code Organization
- Status: draft
- Start Date: 2021-04-15
- Authors: Nathan Stilwell
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# The Problem

The current architecture to share common "page" components for cluster observability across DB Console and CockroachCloud Console makes development and maintenance of those components difficult. The motivation behind moving these components out of a repository containing an application was to not show preference to a specific context; to treat each application as a generic consumer. In practice, this simply isn't true. CockroachDB is the API that drives these components (through protocol buffers) and the majority of the work to create the collection of components known as "Cluster UI" was to mirror these views in CockroachCloud. Sharing these components has been successful and the "Cluster UI" components function in CockroachCloud, but developing these components for DB Console has become more arduous.

The components that are rendered for `/statements`, `/transactions`, and `/sessions` were extracted (the code copied and then refactored) from DB Console into another repo intended to publish UI dependencies. The motivation here was that they would be consumed by CockroachCloud console as a dependency. The _presentational_ components themselves had depenencies on may other presentational components that also needed to be copied. These components also dependend on _connected_ components to take relevant data from the application state and pass them as props to the component. These componets are generally lists of things that _link_ to another component to display an individual item (Statements -> Statement Details, Transactions -> Tranaction Details, etc) and so also depended on the ability to use the applications routing. All of these mechanisms were established in DB-Console, but are incompatible with the CockroachCloud console application. So code had to be created to integrate these features into the application state and routes of CC Console.

As it stands today most of the code that is published in `@cockroachlabs/cluster-ui` (the package containing these components) is to support the features of CockroachCloud Console. In an effort to keep these features consistent between DB Console and CC Console, the presentational components were replaced in DB Console leaving in place the routing and state managment. CRDB still serves as the API for the application state that supports these presentational components which is still housed in CRDB repo (`/pkg/ui/src/js/protos.js`). This creates a crisscross of dependencies that make local development increasingly challenging.

![cluster ui dependency](http://www.plantuml.com/plantuml/png/VP91Yzim48Nl_XM3Jzs3y3sKihlUImjRba9FcnnaQUoeaMQ4D0g4qlzUEqvhXwIzsUXxpzyJVioYavJWAt7Y4UhMbooOuFjdi1YHB98vWIDuUGPD5jeMMgRC8_l1b9IGvA6keZO7FOpnaLgEsHmIQxEtNVFtkOc7eIdkeOQVrVkAVefnLxt7nC9P_SYxwbT5B0WTYt00xc5r21jelKEIivAS8kOj3KYOjA25Fd7OqenFwkstb-T5SLbuhLXSSs5oOsP6_H5Tti6mnSckJpfCRUsTmLvtkGcQnYwhCgQZCpWNZVXwyTXZF1SBzTUtL_YYZpgRvAB0syLa_dgodJhFwKglp7dvRYqrzQolHts2-o7OmAkcehq_GIoTTtcFzdOUgB5XtR-1fo8Sj3XpdTqI4mHC0u8m13q5xlHzLEeVHZTNChFhRTDlyd_Y4ScMu7y3)

For local development, dependency management tools can create symlinks for dependency development. Developing with a single dependency, and thus a single symlink, is fairly trivial and a common practice among developers of published npm packages. Adding layers of dependency, and thus symlinks, increases the difficulty and combining layers of dependency and crisscrossing repos increases it further. This is due to the nature of frontend development tools needing to "watch and rebuild" these symlinked dependencies and so the more symlinks your are managing, the more build processes you are juggling simultanously. From the diagram above you can see that the `cluster-ui` package continues to depend on the `protos-js` in DB Console that need to be published to npm to be consumed as a depenency. For the purposes of local development in DB Console, this means a change to the _backend_ and the _frontend_ for one of these "pages" would require the engineer to create two symlinks and potentially manually rebuild two or three times to see an update in the UI. This is a significant hinderance. This hinderance becomes more exaserbated when considering multiple DB versions.

![cluster ui multiple versions](http://www.plantuml.com/plantuml/png/dP8nRuCm48Lt_ufJfZ1SACnHH4kNLcgLfLF4O8oJqXYCR1nLglBV2t55cjXkf6xeVEzzTu_BT4zLRnf1o5RHXiECIRPNahBtgeodxnby4O2EiZtT3JsF3v3BLal1OTDGtNDLmDmhZBlzBZPrP0q0Dh-azrq7QR982llIATbBZHyQhELOFf8GLd71gUQOtYtIVyC7hGCVYnPzz6PYwgimSc76SU7jQGfZ0ZBxXgO69YiESziGMK38vQoowuNjnkTecfO9QIgOlQvVu9aUg6QgxdfOMuIsBXkcgX9FwvPcYqpKlEqBSc6U3QwBJxUOZCLpAda-fNdENz1Inmrj1sA5cgj3pNZqWyukkoxMnBIuqP9WSg8Jxmt14Yj4tnRkOG4ElQjG5CHt9jRVbyVrwS3Vti6B03IuyiuVbYm7JHWLk1P0It9N2IWi9mmYxj20g71r-tSwGxZAmAralArC8xu1)


# Proposal

This leads me to how we might simplify this situation. Since the only code we are using from `@cockroachlabs/cluster-ui` are the presentational components, by moving the refactored components back into the CRDB codebase there will be no symlinks required to develop features there. By using the CRDB repo as the codebase for the cluster-ui pages used for CockroachCloud, the version of the pages and the underlying API (protos-js) will be controlled with versioned release branches and backports. This will create a great advantage for developing in DB Console as there will be no symlinked dependencies. Local development of CockroachCloud Console will still require symlinking to the CRDB repo, but it will be reduced to a single managable symlink.

![no symlinks](http://www.plantuml.com/plantuml/png/NOynIySm44Vt-nH79lz3wUuWnRe8havjXpIvjh59BYGNGSJlRa7GuFQ3Uxp73KLqbXeyY1DFg4PVYr36rsUqb2XQYkO3NSIdHnmv5hwWkwrApNHJaBGvzKwesgjXSsRMvh1h_xlxTEvzTJU5dp2PdqXNtG9JoYnSkhUom1T45iALHJWWRzYIU7ybBE6MESonZVn1SmpyJXOFoWIj6hk7_x8NDjetJy9mZHhyDm00)

![CC-Console dependency](http://www.plantuml.com/plantuml/png/VP31IWCn48RlUOfXJ_OGzYuYrHL1KUd5Kzj3DfbjOvDCI2O5aRwxiOMwgz1R8F_CztyppwmI5-y4MtO8JUTAPapm_WG68mQ3GN-23T1rq578L4DNG-6ISDQ8gFHERUhOm_tlhoQzIwJqrlJj-Tt6tQOjTjR0brZg4qGMknRBc8Wfou-y5WSHY15wOYOFDC0u2TSLOH26H7uL9f1pQ1KyxT705XEA8IbVgvsJJlMvzIhKmaPcJ7khyLYdgZqc2bnjuIx6dnxQKJ7Gl2rUuLdjLTR9HeR5IJSVf-RJnMEdWnXhWRpzA_92MhdHWVztW1ifuH9Zib5Mn8Lm9IvAxspUeW4c2D6BUIKLxxu1)

This also has the advantage of reducing the discussion of multiple versions to largely a discussion of the version of the database. DB Console will have no consideration of the version of Cluster UI as it will simply be the one in the codebase. Backports will be managed in the same way as other code changes in DB Console, using git branches.

![release branches](http://www.plantuml.com/plantuml/png/VT313e8m3CRn-vwYuJuK7i3m7eH3ALMKmKWxJiPtTxf8ucAyBVzy-MLfem4bRBE3eRVa5_MEhR1ZLBQzu48Zgp5dmPcX84-JUAHnw0_xku2x0LYH9hp4JJkZ1fOkPMXNyS6hlIau3AoX0tk2bjpsMjdjkkUuVfWlwVEFkYgh7zuxvrgA_x1ZMISiqsBFjHGJ5hc6bo6UoZm1)

And in support of multiple cluster versions in CockroachCloud, the published package (`@cockroachlabs/cluster-ui`) could be aligned to the version of the DB for convinience.

![aligned versions](http://www.plantuml.com/plantuml/png/ZP4nRyCW48LtViN9KpgG1F-0ofBdLjsj36UuDccmYG1tgVBVYuNY5brKPqDFxxx7lMj2H1-dmOBffNPZS9Qa9qiOIFOUHtsa8CIxnu6WqawVlKDzDFrkULVmBG0kgC_uaJpTW26IFsGPkejtqGNv6S1Yf10F89-XqtQQNN8wBN9oNqL1klsZLbjrXpVBZ9R5_s3xRPrX9MtMEMqMCopXj7MJWUhrxGKspRA_nJuyPU2VqQPMQPHhYcbpXBWk7RSjjFaEi2aoTWEEQHKZ0_clawPRPbEwVj6fDvdGQnQrPjdB_C5xHvFIfChutlmat9dRu7LpP-v8i9XiNQ-0fJuqgCh1udDhTJKqcccmuWS0)


# Technical design

## Moving the code

Relocating the code from the UI repo into the `ui` package of the CRDB repo will require some adjustments to be made to the build configuration (webpack) to include and ignore portions of the code. Since much of the package is not intended for consumption in DB Console, but will still need to be published as a npm package, we want to ensure that all builds work successfully. I anticipate this will require minor adjustments to Webpack configurations, ignore files, TypeScript configuration, linting, and testing configuration. The versions of major dependencies in `cluster-ui` are pretty well aligned to DB Console (and CC Console) as they are declared as peer dependencies to not conflict or bulk the bundle. But there are most likely other dependencies that will need to be added to DB Console or versions that may need to be adjusted for compatibility. In addition to "moving the code", we also want to preserve the git history from the UI repo. This was done for previous moves and so should include a mostly complete. 


## Updating DB Console

After the code is moved, dependencies have been resolved, and the build is successful the native code that references the `@cockroachlabs/cluster-ui` dependency should be updated to point to local code instead. There are currently around 35 references to the external dependency with most of them being utility components. There will most likely be some component duplication when moving the whole of Cluster UI back into the repo from whence it came. There will be an opportunity to remove identical duplicate components, but I think an evaluation of similar components should be postponed until both applications have been fully updated. 

Once the move and cleanup has happened on master, an update will need to be made to `release-20.2` as this version also depends on the an external `cluster-ui` as well. The version supporting 20.2 is on a separate branch in the UI repo (`cluster-ui-20.2`) and so while the code move on master could be referenced I'm not confident it can be duplicated. Depending on the state of the release, the work on CRDB master may need to be backported to `release-21.1` as well.

## Updating CC Console

When we have confirmed that both DB Console can be built successfully and the Cluster UI package can be build independently, publishing for Cluster UI should be configured. Currently in the UI repo, the `cluster-ui` package is published by a Github Action when pull requests are merged to master. Eventually it would be highly beneficial to have publishing be a part of merging code, we anticipate that immediately after move publishing will be a manual process identical to the publishing of `@cockroachlabs/crdb-protobuf-client`. New versions of `@cockroachlabs/cluster-ui` should be published to match the version of DB Console they represent (21.2.x, 21.1.x, 20.2.x). Documentation for the Cluster UI package should be updated to reflect any significant architectural changes (probably mostly related to protobufs) and describing the new versioning. When the new versions are available on npm, CockroachCloud console should be updated to depend on these new versions. I don't anticipate any other code changes necessary as this proposal does not include any change in functionality of the components.

## Deprecate the package in UI repo

As a final step, the code in `cockroachdb/ui` should be deprecated, the `package.json` set to private, and the Github Action for publishing removed. We should keep the code around for reference or back up until we are confident it is no longer needed.

# Next Step and recommendations for team

## Cluster UI

The Cluster UI team has the most familiarity with the current architecture. To assist in the transition of ownership of these components (to the SQL Observability team) we can commit to doing the bulk of the work mentioned in the above section. Cluster UI would take on moving the code and git commit history into the CRDB repo, updating the build systems, tools, and dependencies required, and configuring npm publishing to allow the SQL <abbr title="observability">o11y</abbr> team to manually publish the package. We could also commit to updating the depedency versions in CockroachCloud console and confirming no unexpected breakage.

## SQL Observability

As the new stewards of the observability components, it is our recommendation to the SQL Observabilty team to consider relocating code specifically engineered for state management and routing in CockroachCloud console to that repo. This should simplify the code in that is pubilshed, the dependency tree, and the code that handles external reducers and routes in that application.

## CockroachCloud Platform

One issue that has been repeatedly encountered by the Cluster UI team in supporting these shared components is resolving conflicts with global styles in CockroachCloud Console. It is our recomendation that the web component paradigmn is best served by removing global css entirely from that application. CC-Console has the ability to use modular css at the component level (CSS Modules). Where styles need to be kept consistent across components, we suggest the use of design tokens or at the very least Sass mixins. 

