#!/usr/bin/env python3
"""Post failures from the current teamcity job as github issues.

Requires the following environment variables:
- TC_API_PASSWORD
- TC_BUILD_BRANCH
- TC_BUILD_ID
- GITHUB_API_TOKEN
"""

import json
import os
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

from urllib.parse import urljoin, urlencode

BASEURL = "https://teamcity.cockroachdb.com/httpAuth/app/rest/"

auth_handler = urllib.request.HTTPBasicAuthHandler()
auth_handler.add_password(realm='TeamCity',
                          uri='https://teamcity.cockroachdb.com',
                          user='robot',
                          passwd=os.environ['TC_API_PASSWORD'])
opener = urllib.request.build_opener(auth_handler)


def tc_url(path, **params):
    return urljoin(BASEURL, path) + '?' + urlencode(params)


def collect_build_results(build_id):
    """Yield a sequence of (name, log) pairs for all failed tests.

    Looks at the given build ID and all its dependencies.
    """
    dep_data = ET.parse(opener.open(tc_url('builds/{0}'.format(build_id),
                                           fields='snapshot-dependencies(build(id,status))')))
    for b in dep_data.findall("./snapshot-dependencies/build"):
        if b.attrib['status'] != 'SUCCESS':
            yield from collect_build_results(b.attrib['id'])

    test_data = ET.parse(opener.open(tc_url('testOccurrences',
                                            locator='count:100,status:FAILURE,build:(id:{0})'.format(build_id),
                                            fields='testOccurrence(details,name,duration,build(buildType(name)))')))
    for o in test_data.findall('./testOccurrence'):
        test_name = '{0}/{1}'.format(o.find('build/buildType').attrib['name'], o.attrib['name'])

        test_log = '--- FAIL: {0}/{1} ({2:.3f}s)\n{3}\n'.format(
            o.find("build/buildType").attrib["name"],
            o.attrib["name"],
            int(o.attrib["duration"])/1000.,
            o.find("details").text)
        yield (test_name, test_log)


def create_issue(build_id, failed_tests):
    """Format a list of failed tests as an issue.

    Returns a dict which should be encoded as json for posting to the
    github API.
    """
    return {
        'title': 'teamcity: failed tests on {0}: {1}'.format(os.environ['TC_BUILD_BRANCH'],
                                                             ', '.join(t[0] for t in failed_tests)),
        'body': '''\
The following tests appear to have failed:

[#{0}](https://teamcity.cockroachdb.com/viewLog.html?buildId={0}):


```
{1:.60000}
```

Please assign, take a look and update the issue accordingly.
'''.format(build_id, ''.join(t[1] for t in failed_tests)),
        'labels': ['test-failure', 'Robot'],
    }


def post_issue(issue):
    req = urllib.request.Request(
        'https://api.github.com/repos/cockroachdb/cockroach/issues',
        data=json.dumps(issue).encode('utf-8'),
        headers={'Authorization': 'token {0}'.format(os.environ['GITHUB_API_TOKEN'])})
    try:
        opener.open(req).read()
    except urllib.error.HTTPError as e:
        # Github's error response bodies include useful information, so print them.
        print('Posting to github failed with error code {0}'.format(e.code))
        print('Error response body: {0}'.format(e.read()))
        print('Request body: {0}'.format(issue))
        raise


if __name__ == '__main__':
    branch = os.environ['TC_BUILD_BRANCH']
    if branch == 'master' or branch.startswith('release-'):
        build_id = os.environ['TC_BUILD_ID']
        failed_tests = list(collect_build_results(build_id))
        if failed_tests:
            issue = create_issue(build_id, failed_tests)
            post_issue(issue)
