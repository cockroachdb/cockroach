import urllib2
import ssl
import sys

cookiefile = sys.argv[1]
url = sys.argv[2]

# Disable SSL cert validation for simplicity.
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Load the cookie.
cookie = file(cookiefile).read().strip()

# Perform the HTTP request.
httpReq = urllib2.Request(url, "", {"Cookie": cookie})
print urllib2.urlopen(httpReq, context=ctx).read()
