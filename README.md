<h1 align="center">
<img src="https://i.imgur.com/K0DIvfk.png" width="200">
</h1><br>

# Datamigrato

Datamigrato is a versatile Python package dedicated to simplifying the process of migrating data across multiple databases, including Cassandra, MongoDB, Firebase, and Firestore. Designed with scalability, ease of use, and a well-maintained code structure in mind, it offers a comprehensive solution for various data transfer strategies and optimizations, making it an ideal choice for projects of all sizes and an accessible platform for contributors.

## Features

- **Wide Range of Database Support:** Migrate data between popular databases such as Cassandra, MongoDB, Firebase, and Firestore.
- **Scalability:** Engineered for scalability to handle large datasets efficiently.
- **Ease of Use:** Simplifies the migration process, reducing the complexity involved in schema conversions, CRUD operations, and connection management.
- **Optimized Data Transfer:** Incorporates advanced data transfer strategies and optimizations to ensure fast and reliable migrations.
- **Well-Maintained Code Structure:** The codebase is organized and documented, facilitating easy understanding and contribution by developers.
- **CI/CD Integration:** Fully integrates into CI/CD pipelines for automated testing and deployment, ensuring code quality and reliability.

## Installation

To install Datamigrato, simply use pip:

```bash
pip install datamigrato
```

## Quick Start

Here's how to quickly get started with Datamigrato:

```python
from datamigrato import Migrator

# Example: Migrate from MongoDB to Cassandra
migrator = Migrator()
migrator.mongo_to_cassandra(source_db_config, target_db_config)
```
Replace source_db_config and target_db_config with your database configurations.

## CI/CD Pipeline Integration

Datamigrato is designed to seamlessly integrate into CI/CD workflows:

- **Continuous Integration (CI):** During the CI phase, code is automatically checked, linted, and tested. We leverage FreeAPI, created by Hitesh Chaudhary and team, for live backend tests. This tool allows us to simulate real-world operations by populating a test database and performing migrations to ensure functionality.
- **Continuous Deployment (CD):** After passing CI tests, the code is reviewed and linted with Flake8 before being published to PyPI, ensuring that only high-quality code is deployed.


## How to Contribute

We welcome contributions from the community! If you're interested in adding support for more databases, enhancing migration algorithms, or otherwise improving Datamigrato, please follow these steps:

1. Fork the repository.
2. Create your feature branch:  
   `git checkout -b feature/AmazingFeature`
3. Commit your changes:  
   `git commit -m 'Add some AmazingFeature'`
4. Push to the branch:  
   `git push origin feature/AmazingFeature`
5. Open a pull request.

## License

Distributed under the MIT License. See [LICENSE](https://github.com/RitikDutta/datamigrato?tab=License-1-ov-file#license-for-datamigrato) for more information.

## Acknowledgments

- A special thank you to [FreeAPI.app](https://freeapi.app) and its contributors for providing an invaluable tool for our CI testing needs.
- For more information on Datamigrato, including detailed documentation and advanced usage examples, please visit our [GitHub repository](https://github.com/ritikdutta/datamigrato).


## Join Us

Datamigrato is more than a tool; it's a community. We're committed to making our project as open and maintainable as possible—because we know that the best ideas come from collaboration. Whether you're fixing a bug or adding a new feature, your contributions help us all.

So, dive in! The code is designed to be accessible, allowing you to easily make your mark. Let's build something great together.

Created with love by Ritikdutta.
[Ritikdutta.com](https://ritikdutta.com)
