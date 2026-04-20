# Workflow Strategy

The Robben Media fork keeps a small, relevant CI surface on Blacksmith-managed
GitHub Actions runners.

## Pull Requests

- `ci.yml` runs repo hygiene checks, README checks, dependency install, and
  Prettier.
- `rust-ci.yml` runs lightweight Rust checks when Rust or workflow files change:
  - `cargo fmt --check`
  - `cargo shear`
- `cargo-deny.yml`, `codespell.yml`, and `blob-size-policy.yml` provide focused
  policy/static checks.

## Removed Upstream Workflows

The upstream OpenAI repo includes release publishing, V8 artifact publishing,
BuildBuddy-backed Bazel/SDK checks, CLA automation, and Codex-powered issue
automation. Those workflows require OpenAI-specific branches, secrets,
release assets, runner groups, or external services, so they are intentionally
not part of this fork's default CI.

## Runner Provider

Supported CI jobs run on Blacksmith-managed GitHub Actions runners. Keep matrix
metadata such as `matrix.runner` and `matrix.os` as logical platform names if
future matrix workflows need them, and route the actual runner label through
`matrix.runs_on`.

Current mappings:

- Linux x64: `blacksmith-4vcpu-ubuntu-2404`
- Linux ARM64: `blacksmith-8vcpu-ubuntu-2404-arm`
- macOS Apple Silicon: `blacksmith-6vcpu-macos-15`
- Windows x64: `blacksmith-4vcpu-windows-2025`
