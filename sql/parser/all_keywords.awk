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
