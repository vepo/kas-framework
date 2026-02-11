# Contributing to Kafka Adaptive Streams (KAS)

Thank you for considering contributing to KAS! This is both a research project and production-ready library, and we welcome contributions from the community.

## 📋 How to Contribute

### Reporting Issues
- **Bug reports**: Include KAS version, Kafka version, JVM version, OS, and a minimal reproducible example
- **Feature requests**: Describe the adaptation rule or enhancement, including motivation and expected behavior

### Code Contributions
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes with clear, descriptive messages
4. Add tests for new functionality
5. Ensure all tests pass (`mvn clean verify`)
6. Push to your branch (`git push origin feature/your-feature`)
7. Open a Pull Request

### Development Setup
```bash
git clone https://github.com/vepo/kas-framework.git
cd kas-framework
mvn clean install
```

### Style Guide
- Follow existing code formatting
- Java: 4 spaces for indentation, 120 character line limit
- Include JavaDoc for public APIs
- Add comments explaining non-obvious adaptation logic

### Research Contributions
If your contribution is part of academic work:
- Cite KAS appropriately in your papers
- Share experimental datasets when possible
- Include experimental results in PR description

## 📜 License
By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

**Questions?** Open an issue or contact the maintainers at [Discussions](https://github.com/vepo/kas-framework/discussions)