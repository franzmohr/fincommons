"""Financial reports: the regimes that require them tagged, and the two
tables that put them side by side.

Three regimes require the periodic reports of listed issuers to be tagged in
XBRL, and each publishes the result in its own way:

``esef``
    the European Single Electronic Format, as filings.xbrl.org publishes it,
``sec``
    the inline XBRL mandate of the Securities and Exchange Commission, as the
    company files of data.sec.gov publish it,
``edinet``
    the Financial Services Agency of Japan, as the EDINET API publishes it,
``combined``
    the three reduced to the same columns and put one after the other.

This file imports none of them, and that is deliberate. Each regime stands on
its own: a lake that collects Japanese filings and nothing else imports
:mod:`fincommons.financialreport.edinet`, and neither the combining layer nor
any other regime is loaded by doing so. The dependency runs one way --
``combined`` reads the three, none of the three reads ``combined``, and none
reads another -- and :mod:`tests.test_combined` holds it to that. A package
that grouped them by importing them here would have taken that away, which is
why this module is a namespace and not a facade.
"""
