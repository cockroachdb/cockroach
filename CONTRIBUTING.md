# Contributing to Cockroach

### Getting and building

A working [Go environment](http://golang.org/doc/code.html) and [prerequisites for building
RocksDB](https://github.com/cockroachdb/rocksdb/blob/master/INSTALL.md) are both presumed.
```bash
go get github.com/cockroachdb/cockroach
cd $GOPATH/src/github.com/cockroachdb/cockroach
./bootstrap.sh
make
```

### Bugs, features, and code review

**We use GitHub for everything but code reviews.**

We use [Phabricator](http://phabricator.andybons.com/) for code reviews. Phabricator
uses your GitHub credentials, just click on the "Login or Register" button. The Phabricator
development model is similar to the GitHub pull request model in that changes are
typically developed on their own branch and then uploaded for review. When a change is
uploaded to Phabricator (via `arc diff`), it is not merged with master until
the review is complete and submitted.

+ Hack away...
+ Commit your changes locally using `git add` and `git commit`.
+ Upload your change for review using `arc diff`.

### Installing Arcanist
To install Arcanist (the code review tool)...

Create a dir that will hold the two repos required.

`$ mkdir somewhere`

then clone the required repos into that folder:

```
somewhere/ $ git clone git://github.com/facebook/libphutil.git
somewhere/ $ git clone git://github.com/facebook/arcanist.git
```

Then add somewhere/arcanist/bin/ to your $PATH

Now within the cockroach directory...

```
$ git checkout -b newbranch
... make changes ...
$ git commit -a -m 'my awesome changes'
$ arc diff
```

Say you’ve updated your diff to account for some suggestions, you just commit those on the same branch and do:

```
$ arc diff
```

again and it will take care of uploading things.

Once you’re ready to land a change (it has been approved).

```
$ arc land
```

it will squash all commits, update the commit message with the original headline and description, and have a link back to the review. It will also clean up (delete) your feature branch.

