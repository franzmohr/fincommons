# Security policy

fincommons is shared infrastructure: everyone who uses it runs the same code
inside their own environment. A vulnerability in it is therefore a
vulnerability for every user, and the advantage of shared code -- that a flaw
is fixed once and the fix reaches everyone -- only holds if flaws are reported
to the maintainers first and disclosed in a coordinated way. This document
says how.

## Reporting a vulnerability

**Do not report a vulnerability in a public issue, a pull request or a
discussion.** Doing so discloses it to everyone before a fix exists.

Report it privately instead, through GitHub's private vulnerability reporting:
on the repository's *Security* tab, choose *Report a vulnerability*. The
report opens a draft security advisory that only you and the maintainers can
see, and the fix can be prepared in a private fork attached to it.

Please include, as far as you can:

- the affected module, function or file, and the version or commit;
- what an attacker could do and under which conditions -- for instance, what
  they would need to control (a downloaded file, a configuration entry, a
  delivery placed in the landing zone);
- the steps or a minimal example that reproduces it, using public or synthetic
  data only. **Never include confidential data or credentials in a report.**

## What happens next

The maintainers aim to:

| Step | Target |
| --- | --- |
| Acknowledge the report | within 5 working days |
| Assess it and tell you whether it is accepted as a vulnerability | within 10 working days |
| Release a fix | as soon as practicable, by severity; critical issues first |
| Disclose it publicly | once a fix is released, and at the latest 90 days after the report unless agreed otherwise |

The 90-day window is the customary one for coordinated disclosure. It can be
shortened when a vulnerability is already being exploited, and extended by
agreement when a fix needs more time. You will be kept informed throughout,
and credited in the advisory unless you prefer otherwise.

A fixed vulnerability is published as a GitHub security advisory and noted in
the release that fixes it, so that everyone running fincommons can
see that it has to update.

## Supported versions

fincommons is at version 0.x. Until 1.0, security fixes are made on the
latest release and on `main`; older 0.x releases are not patched. Users
should therefore update to the latest release when an advisory is published.

## Scope

In scope is everything in this repository: the package `fincommons`, its
tools, its build configuration and its documentation where it gives advice
that would be unsafe to follow.

Out of scope, but still worth telling us about:

- **Vulnerabilities in dependencies** (pandas, PySpark, delta-spark, requests
  and others). Report them to the project concerned. Tell us as well if the way
  fincommons uses a dependency exposes its users, or if a dependency needs a
  higher minimum version.
- **The data sources** the pipelines download from. A publisher serving a
  malformed or malicious file is reported to the publisher; tell us if
  fincommons fails to refuse such a file.

## How fincommons limits its own exposure

Knowing what the package does and does not do helps to judge a report:

- It is a library that runs in batch inside its user's own environment.
  It opens no network port and runs no service.
- It downloads public data over HTTPS from the publishers named in its module
  documentation, with timeouts, and refuses incomplete or empty answers where
  that is described; FIRDS files are additionally checked against the MD5
  digests ESMA publishes.
- It reads and writes only the locations named in `paths.yaml`, which it
  trusts: whoever can edit that file can direct the pipelines anywhere the
  running user may write.
- It contains no confidential data and needs no credentials; it never stores
  any.
- Its dependencies are few and widely used, declared with lower bounds only,
  so that its user controls the versions installed and can apply their
  own software composition analysis.
