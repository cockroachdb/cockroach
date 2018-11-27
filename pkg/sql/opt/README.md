The `opt` package defines common high-level interfaces and classes that multiple
sub-packages share. None of these definitions depend on any sub-packages, so all
sub-packages can therefore depend on `opt` without causing cyclical dependencies
(which Go does not allow).
