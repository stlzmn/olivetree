# Contributing

Thanks for considering a contribution.

## Ground Rules

- Be respectful in issues and PR discussions.
- For larger changes, open an issue first to discuss direction.
- Keep PRs focused and small when possible.

## Development Setup

```bash
cargo fmt
cargo check
cargo test
```

## Pull Request Checklist

- Code is formatted with `cargo fmt`.
- Project builds with `cargo check`.
- Tests pass with `cargo test`.
- New behavior includes tests where practical.
- Public API/behavior changes are documented in `README.md` (if applicable).

## Commit Style

No strict convention is required. Clear, descriptive commit messages are preferred.

## Reporting Bugs

Please open an issue with:
- expected behavior
- actual behavior
- reproduction steps
- environment details (OS, Rust version)
