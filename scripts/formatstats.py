#! /usr/bin/env python3
#
# This script produces the HTML visualization
# of the output produced by repostats.py.
#
# Use with: python3 scripts/formatstats.py stats.py >stats.html
#

import sys

with open(sys.argv[1]) as fstats:
    stats = eval(fstats.read())

# Merge X/ into pkg/X if it exists
removed = []
for path, ustats in stats.items():
    pkgpath = 'pkg/' + path
    if pkgpath in stats:
        print("MERGING", path, pkgpath, file=sys.stderr)
        pkgstats = stats.get(pkgpath, {})
        stats[pkgpath] = pkgstats
        for user, ystat in ustats.items():
            pkgustats = pkgstats.get(user, {})
            pkgstats[user] = pkgustats
            for y, v in ystat.items():
                pkgystats = pkgustats.get(y, 0)
                pkgystats += v
                pkgustats[y] = pkgystats
        removed.append(path)
for path in removed:
    del stats[path]
removed = []
# Remove X* if there is no X/Y
pstars = [x for x in stats.keys() if x.endswith('*')]
print("WOO", pstars, file=sys.stderr)
for spath in pstars:
    found = False
    for p in stats:
        if p == spath:
            continue
        # print("CMP", p, spath, file=sys.stderr)
        if p.startswith(spath[:-1]) and len(p) >= len(spath):
            found = True
            break
    if found:
        continue
    print("TRIM", spath, file=sys.stderr)
    removed.append(spath)
for path in removed:
    del stats[path]

print("""
<html>
<head>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.0/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></script>
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.0/js/bootstrap.min.js"></script>
</head>
<body>
<div class=container>""")

periods = set()
for _, d in stats.items():
    for _, d2 in d.items():
        for y in d2:
            periods.add(y)
periods = [x for x in periods]
periods.sort()

print("<div class=row><div class=col-sm-4>PATH</div><div class=col-sm-1>PERSON</div>")
for p in periods[::-1]:
    print("<div class=col-sm-1>" + p + "</div>")
print("</div>")

allpaths = [x for x in stats.keys()]
allpaths.sort()

def time_weight(series, allstat):
    sum = 0
    for i, p in enumerate(periods[::-1]):
        data = series.get(p,0)
        max = float(allstat.get(p,0))
        if max == 0:
            continue
        w = 1./(i+1)
        sum += w + data/max
    return sum

def color(v, max):
    if max == 0:
        return '#f'
    ratio = 255 - (255 * v / max)
    c = '%02x' % int(ratio)
    return "#%s%s%s" % (c,c,c)

pathct = 0
for path in allpaths:
    ustat = stats[path]
    allstat = ustat['__all__']
    allsum = sum(allstat.values())
    if allsum == 0:
        continue
    if not path:
        path = "TOP"
    print(("<div class=row><div class=col-sm-4><a href='#p%d' data-toggle=collapse>" % pathct) + "" + path + "</a></div>")
    print("<div class=col-sm-1>__all__</div>")
    for p in periods[::-1]:
        d = allstat.get(p, 0)
        print("<div class=col-sm-1 style='background-color:%s'>&nbsp;</div>" % color(d, allsum))
    print("</div>")
    print("<div id=p%d class=collapse>" % pathct)
    usums = [(user, time_weight(ystat, allstat)) for user, ystat in ustat.items() if user != '__all__']
    usums.sort(key=lambda x:-x[1])
    for user, _ in usums:
        ystat = ustat[user]
        if user.startswith('?'):
            contact = 'mailto:' + user[1:]
        else:
            contact = "http://localhost:5000/v1/person/" + user + "/profile"
        print("<div class=row><div class=col-sm-4>.</div><div class=col-sm-1><a href='" + contact + "'>" + user + "</a></div>")
        for p in periods[::-1]:
            d = ystat.get(p, 0)
            allsump = allstat.get(p, 0)
            print("<div class=col-sm-1 style='background-color:%s'>&nbsp;</div>" % color(d, allsump))
        print("</div>")
    print("</div>")
    pathct += 1

print("""</div></body></html>""")
