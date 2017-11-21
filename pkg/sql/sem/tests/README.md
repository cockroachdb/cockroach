This directory contains SQL front-end tests that cannot be placed in
their respective front-end packages because a circular dependency
would be otherwise necessary.

This is typical of tests that exercise some tree aspect (e.g. type
checking) but need the SQL parser to input test trees. As the `parser`
package depends on `tree`, a tests that needs both cannot live in
`tree`. We do not wish to place such tests in the `parser` package to
keep `parser` as small as possible.
