# Changelog

## [Unreleased]

### Changed
- **Config**: Restored pipeline stages in `config/example.json` to include `status == "active"` filtering and `processed_at` timestamp injection. This ensures downstream consumers receive only active records and have necessary temporal metadata.
