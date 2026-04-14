# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- Removed unnecessary Splink input rematerialisation of `address_without_numbers`, which could trigger large DuckDB temporary spill files before `predict()`.  Rough 2x speed boost when matching to full UK dataset.

## [1.0.1] -

### Fixed


## [1.0.0] - 2026-03-04

Initial stable release.

[Unreleased]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/moj-analytical-services/uk_address_matcher/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/moj-analytical-services/uk_address_matcher/releases/tag/v1.0.0
