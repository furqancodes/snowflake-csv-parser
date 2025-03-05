# Snowflake CSV Parser

## Overview
The Snowflake CSV Parser is a tool designed to parse CSV files and load the data into Snowflake, a cloud-based data warehousing service. This project aims to simplify the process of importing CSV data into Snowflake, ensuring data integrity and ease of use.

## Features
- Parse CSV files with various delimiters
- Handle large CSV files efficiently
- Validate data before loading into Snowflake
- Support for custom data transformations
- Detailed logging and error reporting

## Prerequisites
- Node.js 14 or higher
- Snowflake account
- Snowflake Node.js Driver

## Installation
1. Clone the repository:
  ```sh
  git clone https://github.com/furqancodes/snowflake-csv-parser.git
  ```
2. Navigate to the project directory:
  ```sh
  cd snowflake-csv-parser
  ```
3. Install the required dependencies:
  ```sh
  npm install
  ```

## Configuration
1. Create an `.env` file in the project root with the structure mentioned in the `.env.example` file.

## Usage
1. Place your CSV files in the `dist/data` directory.
2. Build the project:
  ```sh
  npm run build
  ```
3. Run the parser script:
  ```sh
  npm run start
  ```
4. Monitor the logs for progress and any errors.

## Contact
For any questions or feedback, please open an issue on the [GitHub repository](https://github.com/furqancodes/snowflake-csv-parser).

