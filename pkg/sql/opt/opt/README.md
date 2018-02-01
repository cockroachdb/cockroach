This is a temporary directory. The files in this directory will soon be moved
to the main "opt" directory, one level above. However, before this can happen,
we need to convert the existing files in "opt" to use the new opt code. As part
of that, those files will move to the appropriate subdirectories. The only
files in "opt" will be the files in this temporary directory (interfaces, high-
level definitions, etc).

These files are broken out so that various sub-packages can use each other in
tests. For example, the builder package wants to test using the factory from
the xform package, and the xform package wants to test using the builder from
the builder package. Golang doesn't allow cyclical dependencies, so the way to
break the cycle is defining a set of common interfaces and classes that the
packages use to communicate with one another in production code. These files
should only include simple interfaces and definitions that do not depend on any
sub-packages.
