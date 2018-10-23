/^(cockroachdb_extra_)?reserved_keyword:/ {
  keyword = 0
  next
}

/^.*_keyword:/ {
  keyword = 1
  next
}

/^$/ {
  keyword = 0
}

{
  if (keyword && $NF != "") {
    print $NF
  }
}
