These files are broken out so that various sub-packages can use each other in
tests. For example, the builder package wants to test using the factory from
the xform package, and the xform package wants to test using the builder from
the builder package. Golang doesn't allow cyclical dependencies, so the way to
break the cycle is defining a set of common interfaces and classes that the
packages use to communicate with one another in production code. These files
should only include simple interfaces and definitions that do not depend on any
sub-packages.
