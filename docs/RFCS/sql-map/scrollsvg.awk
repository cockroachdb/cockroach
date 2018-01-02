/<\/body>/ {
    print "<script>"
    print " window.onload = function() {"
    print "   window.zoomSvg = svgPanZoom('#svg-items', {"
    print "     zoomEnabled: true,"
    print "     controlIconsEnabled: true,"
    print "     fit: true,"
    print "     center: true,"
    print "   });"
    print " }"
    print "</script>"
}

{ print $0 }

/<head>/ {
    print "<script src=\"https://ariutta.github.io/svg-pan-zoom/dist/svg-pan-zoom.js\"></script>"
}
