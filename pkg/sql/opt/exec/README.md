This `exec` package is temporarily in the `opt` directory. Eventually, it will
be moved to the `sql` directory, and we will move other execution-related
packages into the `sql/exec` directory as well. Until then, it's better to keep
it here so that developers are not confused as to why we have a top-level
`exec` directory that doesn't actually contain the main body of execution code.
