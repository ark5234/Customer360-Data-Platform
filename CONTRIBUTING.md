# Contributing to Customer360 Data Platform

Thank you for your interest in contributing! This document provides guidelines for contributing to this project.

## How to Contribute

### Reporting Issues

- Use GitHub Issues to report bugs or suggest features
- Provide detailed descriptions with steps to reproduce
- Include relevant logs, screenshots, or error messages

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes
4. Test thoroughly
5. Commit with clear messages
6. Push to your fork
7. Open a Pull Request

### Code Style

- **Python**: Follow PEP 8
- **SQL**: Use lowercase keywords, CamelCase for table names
- **Documentation**: Use Markdown with clear headings

### Testing

Before submitting:
- Run all Airflow DAGs manually
- Execute dbt tests: `dbt test`
- Verify Spark jobs complete without errors
- Check data quality results

## Development Setup

See `docs/QUICK_START.md` for local setup instructions.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
