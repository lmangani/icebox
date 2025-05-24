# Icebox Integration Testing

This document outlines the strategy and guidelines for integration testing in the Icebox project.

## 1. Overview

Integration tests are designed to verify the interactions between different components of Icebox, ensuring they work together as expected. These tests cover end-to-end scenarios, from CLI commands to data manipulation and querying.

## 2. Test Environment

### 2.1. Setup

Integration tests are executed within a controlled environment. Each test run should:

1. **Initialize a temporary Icebox project**: This includes creating a temporary directory for the project, initializing a catalog (e.g., SQLite), and configuring storage (e.g., local filesystem or embedded MinIO).
2. **Prepare test data**: Test data, such as Parquet files, should be copied or linked to the temporary project's data directory. The `flights.parquet` file in `icebox/testdata` is available for use.
3. **Ensure clean state**: Each test should start with a clean environment to avoid interference from previous test runs.

### 2.2. Teardown

After each test run:

1. **Clean up resources**: Remove the temporary Icebox project directory and any other temporary files or services created during the test.

## 3. Test Structure

Integration tests are located in the `icebox/integration_tests` directory.

* **`main_test.go`**: Contains the main test runner, setup/teardown logic (e.g., `TestMain`), and helper functions.
* **`*_test.go` files**: Individual test files grouped by functionality (e.g., `cli_test.go`, `import_test.go`).

Each test case should follow standard Go testing conventions.

## 4. Running Tests

To run the integration tests:

```bash
cd icebox/integration_tests
go test -v ./...
```

## 5. Writing New Tests

When adding new integration tests:

1. **Identify key scenarios**: Focus on testing interactions between components and end-to-end user workflows.
2. **Use helper functions**: Leverage helper functions in `main_test.go` for common tasks like project initialization, running CLI commands, and asserting outcomes.
3. **Ensure idempotency**: Design tests to be repeatable and produce consistent results.
4. **Clean up resources**: Make sure all resources created during a test are cleaned up, even if the test fails.
5. **Document tests**: Add comments to explain the purpose and steps of each test case.

## 6. Test Coverage

The goal is to cover critical functionalities, including:

* Project initialization (`icebox init`)
* Data import (`icebox import`)
* SQL querying (`icebox sql` and `icebox shell`)
* Table operations (`icebox table ...`)
* Catalog operations (`icebox catalog ...`)
* Time-travel queries
* Embedded MinIO (if applicable)
* Pack and Unpack operations

## 7. Continuous Integration

Integration tests should be part of the CI pipeline to ensure that changes do not break existing functionality.

## 8. Example Test Workflow (using `flights.parquet`)

1. **Setup**:
    * Create a temporary directory (e.g., `/tmp/icebox-test-xxxx`).
    * Run `icebox init` in the temporary directory.
    * Copy `testdata/flights.parquet` to the appropriate data location within the temporary project.
2. **Test `icebox import`**:
    * Run `icebox import flights.parquet --table flights_test`.
    * Verify that the table `flights_test` is created.
    * Check the table schema and row count.
3. **Test `icebox sql`**:
    * Run `icebox sql "SELECT COUNT(*) FROM flights_test"`.
    * Verify the output matches the expected row count.
    * Run a more complex query (e.g., `SELECT origin, COUNT(*) FROM flights_test GROUP BY origin`).
    * Verify the results.
4. **Test `icebox table describe`**:
    * Run `icebox table describe flights_test`.
    * Verify the output shows correct schema, location, and properties.
5. **Test `icebox table history`**:
    * Run `icebox table history flights_test`.
    * Verify the output shows the initial append operation.
6. **Test `icebox time-travel`**:
    * (After some modifications if applicable, or test with initial snapshot)
    * Run `icebox time-travel flights_test --as-of <snapshot_id_or_timestamp> --query "SELECT COUNT(*) FROM flights_test"`.
    * Verify the result.
7. **Teardown**:
    * Remove the temporary directory.
