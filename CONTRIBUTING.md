# Contributing

Thank you for considering contributing to tg-session-gateway!

## How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/amazing-feature`)
3. Make your changes
4. Run tests (`make test` or `pytest -q`)
5. Run linter (`make lint` or `ruff check gateway/ mtgateway/`)
6. Commit with a clear message
7. Open a Pull Request

## Code Style

- Python 3.11+
- Line length: 100 characters
- Formatter: `ruff format`
- Linter: `ruff check`
- Type checking: `mypy --strict` (target for gateway/ and mtgateway/)
- Import sorting: `ruff` IS rules

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):
- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `test:` test changes
- `refactor:` code structure changes
- `chore:` maintenance

## Testing

- All new features require tests
- Bug fixes require a test that reproduces the bug
- Integration tests go in `tests/integration/`
- Unit tests go in `tests/`

## Security

- Never commit tokens, auth_keys, or secrets
- Report vulnerabilities to security@ (see SECURITY.md)
- Do not open public issues for security matters

## Questions?

Open a GitHub Discussion or an issue with the `question` label.
