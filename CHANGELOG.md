# Changelog

The notable changes of each release. The project follows
[semantic versioning](https://semver.org/); while the version is 0.x, a minor
release may change the public API, which `tests/test_public_api.py` lists.

## Unreleased

- `marketpricedata.curves`: risk-free zero curves and discount factors per
  currency from the overnight rates and OIS quotes of the Silver table, with
  log-linear interpolation in the discount factors, parallel shifts and
  forward rates. Moved from the forward-looking analysis of EMIR in
  finsupverse, which now imports it from here.

## 0.1.0

The first release.

- `exchangerate`: the reference rates of the ECB and the BIS, combined into one
  table per calendar day and currency pair, and conversion of amounts in
  Spark, pandas and polars frames.
- `marketpricedata`: prices and rates from public publishers -- the ECB, the
  Federal Reserve Bank of New York, the Bank of England, the LBMA, Yahoo
  Finance, the Dhaka Stock Exchange, VNDirect and European power exchanges --
  in one table.
- `firds`: ESMA's FIRDS reference data, from the register's files to the
  historical database ESMA's instructions describe.
- `entitymasterdata`: who a legal entity is, from GLEIF and the registers of
  the ECB, EIOPA and ESMA.
- The infrastructure the modules share: addressing through `paths.yaml`,
  Silver tables rebuilt in full as Parquet or Delta, logging, the file layout
  of tables, declared schemas and a catalogue of what each column holds.
