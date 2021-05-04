# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [5.7.5] - 2021-04
### Fixed
- Problem with pip and cryptography and updated packages' versions

## [5.7.4] - 2020-04
### Added
- Enable multi-line option for append load
  
## [5.7.3] - 2021-02
### Added
- Make init condensation optional, but true by default.
  
## [5.7.2] - 2020-02
### Added
- Modify append load to support more complex partitioning strategies without file_regex
- Added support for configuring write load mode and num output files in append load
- Support for specifying the quote and escape characters. More info on how to specify those here: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html

## [5.7.1] - 2020-01
### Added
- Parameter source_dir_suffix was added to enable us to read simultaneously from several nested "folders" inside data/. Ex: data/20200101/filename.parquet

## [5.7.0] - 2020-01
### Added
- Support for multiple partition attributes (non date-derived) and single non date-derived partition attributes.