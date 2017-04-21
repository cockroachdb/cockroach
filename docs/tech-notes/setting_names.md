# Cluster settings and naming them

Original author: knz & dt

## Introduction

"There are only three hard problems in computing science: naming
things and off-by-one errors."

So say you want to name that configuration flag, what names should you
give it?

## Current state of things

As explained in the settings RFC we have a cluster-wide configuration
system with a shared namespace.
At the time of this writing there are already a couple configurable
things this way, and a list of about fifty more to come.

## Proposed consensus

Based on examples:

```
kv.snapshot.recovery.rate
```

- `kv`: the top-level architecture layer in CockroachDB
- `snapshot.recovery`: the tuning knob
- `rate`: the type of the value

```
sql.trace.session.eventlog.enabled
sql.trace.txn.threshold
```

- `sql`: the top-level architecture layer
- `trace`: the sub-system that's being configured (tracing)
- `session.eventlog`, `txn`: the tuning knob / part
- `enabled`, `threshold`: the type of the value

That gives us the general structure:

**overall-layer dot thing-being-configured dot type**

### Naming the last part

- `rate`: for things like max bytes/second (I'd have argued in favor of `maxrate`, but oh well)
- `enabled`: for a feature
- `threshold`: for things like min value before something happens

### Naming boolean things

A name like `session.logging.enabled` sounds right, whereas a name
like `session.show-log.enabled` sounds a bit awkward/verbose. What's
going on exactly?

This is a matter of grammatical structure:

- if the thing that's being configured is a feature **noun**, then
  it's not clear what a boolean value would do to it. So what comes
  afterwards must be "enabled" to clarify.
- if the thing is described by a **verb**, then a boolean implicitly
  says "do" or "do not do" that verb.

This is what Mozilla has adopted, compare:

- "Noun flags": `layers.async-pan-zoom.enabled`, `media.peerconnection.enabled`, `security.insecure-password.ui.enabled`
- "Verb flags": `alerts.showFavIcons`, `accessibility.warn_on_browsewithcaret`, `layers.acceleration.draw-fps`

## Some context for understanding why this matters

How much we should think about settings name is really a design
spectrum.

At one end of the spectrum, you could simply name a new setting with a
random string of numbers. Or your name followed by the current
date. Some immediate reasons why this is a bad idea:

- if you find this setting already configured some time later, you
  won't remember what it means.
- when you ask someone to list all the settings they have currently
  configured, it's going to be a lot of work to figure out what their
  configuration really is.
- if you tell someone to configure something over the phone, or with
  your handwritings, chances are they will spell something wrong and
  configure something else than they intended.

So naming should satisfy a couple high-level criteria:

- it must use words that can be shared in an audio conversation;
- when found on its own it must give some idea of where to look to get
  more information about what it configures.

At the complete end of the design spectrum we have a committee of 3+
linguists that analyze all the code around the setting being created,
analyze the possible names that will unambiguously refer to the thing
and check in 10+ different human languages with random user trials
that it won't be misunderstood. That would give very good names,
likely, but also be very expensive money- and time-wise.

So right now we're considering a flat namespace with conventions
new names. That's like Mozilla.

## What others do

- Mozilla: single namespace, happy-go-lucky name soup with some conventions.
- Windows registry: top-level sections determined by vendor, name soups in each sections with no structure
- FreeBSD (/etc/rc.conf): programname-underscore-configflag
- GConf: top-level sections determined by vendor, lots of committee
  work to organize the hierarchy in a way that's interoperable between
  apps
