# DB Console Architecture

## Language, Tools, and Frameworks

This repository uses the Typescript language, and the React, and Redux libraries to
build a large complex UI app. Familiarity with these 3 technologies is critical
to understanding what's going on and making changes with intention.

All three technologies have wonderful documentation which you're encouraged to
start with when learning.

### Why it's hard
> A big obstacle to understanding how React and Redux work is the fact
> that both libraries **hide the call graph** from your application.
> This can make it hard to mentally trace code because the calls connecting
> portions of your app are simply not in your codebase. You are required
> to understand _some_ React and Redux internals to make the application's 
> architecture legible.

### React

**Why?** React is a library for building interactive UIs. It lets us use a 
"Component" pattern to define composable views based on what data inputs we have 
and then the library takes care of updating those views when the data changes.

The React docs are very good at quickly walking you through what you need to 
know. You can either follow a 
[Practical Tutorial](https://reactjs.org/tutorial/tutorial.html) or a tour of 
[Main Concepts](https://reactjs.org/docs/hello-world.html). Both are great, not 
too long, and will teach you what you need to be productive.

**Note: It will be hard for you to make progress if you don't understand JSX syntax, and 
what "props" and "state" are with respect to React Components.**

In addition, the [Thinking in React](https://reactjs.org/docs/thinking-in-react.html)
page contains some great paradigm-shifting ideas and also links to the [props vs state](https://reactjs.org/docs/faq-state.html#what-is-the-difference-between-state-and-props) FAQ 
question that is usually the first source of confusion for new developers.

The [React lifecycle diagram](https://projects.wojtekmaj.pl/react-lifecycle-methods-diagram/)
is a great tool for understanding how simple React's mental model can be. The key
to understanding how the Component model works is knowing that anytime the props
or state change within your Component, the React runtime will automatically call
`render()` again on your component and all the components underneath it in the
hierarchy to determine if anything new should be rendered to the DOM.

### Redux

**Why?** Redux is a library for managing complex application state in your UI. We
use it to retrieve and store all the information DB Console needs from CRDB in 
one big tree of data and then slice portions of it out to feed into React 
Components for rendering. In addition, any user interaction with the app that 
requires interaction with CRDB, will almost certainly pass through the Redux
framework.

The Redux docs are very detailed but approachable and use simple examples. It is
recommended that you work through the [Overview](https://redux.js.org/tutorials/fundamentals/part-1-overview) to get a high-level understanding.
In particular the [data flow diagram](https://redux.js.org/tutorials/fundamentals/part-1-overview#data-flow) can help with the basics.

**Note: it will be hard for you to make progress if you don't understand what the
store, reducers, and actions are at a high level.**

For the most part, change you make to DB Console will involve reading in new data
from existing endpoints in order to render something different. Your focus should
be on the Redux **selectors** which are functions of the global state.

More specifically, we use the [`reselect` library](https://redux.js.org/recipes/computing-derived-data#creating-a-memoized-selector) that provides helpers for
automatically creating memoized selector functions of our global state. For
example, one place where we use these is in the Statements Page to compute the
data we need to render inside our table. The data provided by CRDB isn't in a
 format that can be directly put into the table component so we need to do some
pre-processing on it (whether the UI should really be responsible for this is
outside the scope of this doc but that's certainly a good question to ask!).

#### Concrete example: Statements Page

Here's a diagram that can help explain how the data on the Statements Page table
gets there:

![@startuml
title "How does the Statements Page get its data when the page loads?"
React -> "StatementsPage.tsx": render()
React -> "StatementsPage.tsx": componentDidMount()
"StatementsPage.tsx" -> Redux: Dispatch refresh()
Redux -> "cachedDataReducer.ts/refresh": refresh()
"cachedDataReducer.ts/refresh" -> Redux: Dispatch action: cockroachui/CachedDataReducer/statements/REQUEST
Redux -> "cachedDataReducer.ts/reducer": Run reducer with REQUEST action
"cachedDataReducer.ts/reducer" -> Redux: Returns new state for cachedData/statements with `inFlight: true`
"cachedDataReducer.ts/refresh" -> CRDB: HTTP call to API endpoint
CRDB -> "cachedDataReducer.ts/refresh": Invoke callback on response
note right
async
end note
"cachedDataReducer.ts/refresh" -> Redux: Dispatch action: cockroachui/CachedDataReducer/statements/RECEIVE\nwith payload containing statements data
Redux -> "cachedDataReducer.ts/reducer": Run reducer with RECEIVE action
"cachedDataReducer.ts/reducer" -> Redux: Returns new state for cachedData/statements subhierarchy with\nnew payload added to state
Redux -> Redux: Merges cachedData/statements with entire application state
Redux -> "StatementsPage.tsx/selectStatements": Triggers recomputation of selectors that\nread Statements data
note right
When selectors are defined
using `createSelector` Redux
is able to track which portion
of the state can trigger their
recomputation when it changes
end note
"StatementsPage.tsx/selectStatements" -> "StatementsPage.tsx/selectStatements": New version of processed\nStatements data is generated
"StatementsPage.tsx/selectStatements" -> React: Output of selectStatements\nis a prop of the StatementsPage\ncomponent so once it's changed\nReact triggers a re-render
note right
The `selectStatements` selector
is bound to the component's props
using the `connect` function in
StatementsPage.tsx which links
the output of selectors to props
the Component expects as input
end note
React -> "StatementsPage.tsx": render()
@enduml](http://www.plantuml.com/plantuml/png/lLL1SzfC3BtxLsYuV5yFANSERLgWanpIfa3R2mUMNO5tSBIUNJaa_xxIujW9pIHqEkq9izBJUtgIr-U9JUJcfYhOSuKmk0XxS04JS8amPyDuWyG9hiqMOOiCNluummRs9LBEgZLK1UFI-q4nGsCPpjx1e0ShzYsdky488fB3-F-Rr_9ikAa3oU74kwlG40lakKojC4FNt8rWubDjs9R2iOcOIa7aI2QnnfRe9g9Rpon6GG_RHA7h8IzcFaidVVX0AjdkOX1quuVZuoB3r6aVpgPVlqtdYzVLvKTHDsi8sd-mzrn2Mw6bBbx6zvhbXvj82GZta0N19aJeqOzK7eXMdZvLVblo23Wsk2fEy6SyctmSmSLYSIsLgmeum8VhIq1oTV34XSPFcSabtOOTvXfhOtSGr8HK1qfOK624gC8A09FkoHPI7_JutqnmFBtyFbrIDgaszxhz0YSsdZnjeS_DxyeVZJfJ_TLHfsPTUemcsl8-iov9O5rVnZbqEiOCwNjfcQumRZ6zj4Nov2E2gUlAMwDz79TwvjKU9gpGSXyGTnOoyYt6117rWcZuK2niu90S9CIbuIL55E7peoaysPeV9T8Zc1613ZUUq4cmIJh5bPKoZFCsQNNeMC9UyjSLgYSSTJVtfRUo227c8O4guX9RuwqXu8DoFVMnWAC6ybNg6MnfIBpiT_aaNtx3mCyorbini7MjZi5YIkYMTEILjhX5mYYdxdGP-LOVmPU6fJTbECvQadgdnFM3IKzhBwcx-Y4526GHFF-NMcz4QUPuC5IBHJmxV5QU3dWXjLV7_Ajkv8SnhaD3kjjPISSiTAemTPl0Mii68e6kODEGpNFpEkjVlMdNeVAqqn8A3hqZ_QQ6ZaLJnbtVU5TBIWAJX45W_JwS-dKzbmVvgFy4)

#### Q: I have new data coming in through an existing endpoint, how do I access it?
You may need to modify a **selector** function to pass the new piece of data down
to the component's props for rendering. Otherwise, it may just transparently be
available in your component's props if it's simply adding a new field and your
selectors aren't radically changing the data shape after it comes back from the
API.

#### Q: I have a new endpoint I want to hook up to
You will need to define a new `cachedDataReducer` instance or something similar
if you want to add a big chunk of data to the app state. See `apiReducers.ts`
for examples of how these are defined.

#### Q: What does the global application state look like?
You can see this using the Redux DevTools plugin. Open DB Console, then launch
DevTools, click on the Redux tab, and then click "State" in the section selector,
and then the "Tree" tab right below. This will show you an interactive tree with
the application state.

#### Q: What is the `CachedDataReducer`?
This is a small internally created library that manages API calls to CRDB and 
tracks their execution with Redux. It makes the access of endpoints somewhat
uniform in nature. In addition the library can automatically refresh the data
for you and report errors if it can't be retrieved etc.

_Note: Many Redux "best practices" are quite new so documentation you read online
likely won't reflect the patterns you see in this codebase today. It's up to you
to assess how valuable it might be to refactor or write new Redux code to build
your feature_
