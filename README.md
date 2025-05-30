# hackathon_ETLE
Git Repository for Codes related to Hackathon Project

# ETL Configuration Generator

A web-based ETL (Extract, Transform, Load) configuration generator that allows users to create and manage ETL configurations through an intuitive user interface. The tool supports MySQL data extraction, various transformations, and flexible output formats.

## Features

- **Extract Configuration**
  - MySQL database connection
  - Column selection and type specification
  - JSON field extraction with nested path support
  - WHERE conditions with multiple operators
  - ORDER BY and LIMIT clauses

- **Transform Operations**
  - Arithmetic operations (add, multiply, subtract, divide)
  - Aggregate operations (sum, mean, count)
  - Filter operations with various comparison operators
  - Custom file transformations
  - Group by functionality

- **Output Formats**
  - CSV
  - JSON
  - Custom column mapping
  - Nested JSON structure support

## Prerequisites

- Python 3.9 or higher
- MySQL server
- Required Python packages:
  - pandas
  - mysql-connector-python
  - numpy

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd hackathon_ETLE
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Project Structure

```

## Usage

1. **Generate Configuration**
   - Open `UI/index.html` in a web browser
   - Fill in the database connection details
   - Configure columns, transformations, and output settings
   - Click "Generate Configuration" to download the config file

2. **Run ETL Process**
   - Copy the generated `config.json` to the project root directory (same level as `main.py`)
   - Run the ETL process:
   ```bash
   python main.py config.json
   ```

## Configuration Guide

### Database Connection
```json
{
  "extract": {
    "source": {
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "database": "your_database",
      "user": "your_username",
      "password": "your_password",
      "table": "your_table"
    }
  }
}
```

### Column Configuration
- Regular columns:
```json
{
  "name": "column_name",
  "type": "string|date|numeric",
  "output_name": "custom_name"
}
```

- JSON columns:
```json
{
  "name": "json_column",
  "type": "json",
  "keys": [
    {
      "path": "nested.field",
      "output_name": "custom_name"
    }
  ]
}
```

### Transformations
- Filter operation:
```json
{
  "type": "filter",
  "column": "column_name",
  "operator": ">|<|>=|<=|==|!=",
  "value": "value",
  "value_type": "string|number|date"
}
```

- Aggregate operation:
```json
{
  "type": "aggregate",
  "columns": ["column_name"],
  "output_column": "result_name",
  "operation": "sum|mean|count",
  "group_by": ["group_column"]
}
```

## Important Notes

1. **Configuration File Location**
   - The `config.json` file must be in the same directory as `main.py`
   - Use the UI to generate the configuration file
   - Ensure all paths in the configuration are relative to the project root

2. **Data Types**
   - When using filter operations, ensure the `value_type` matches the column type
   - For numeric comparisons, use `"value_type": "number"`
   - For date comparisons, use `"value_type": "date"`

3. **JSON Fields**
   - Use dot notation for nested JSON paths (e.g., "user.address.city")
   - JSON-extracted columns cannot be used in WHERE conditions
   - Always specify output names for JSON fields

4. **Output Paths**
   - Use relative paths for output files
   - Create the output directory if it doesn't exist
   - Ensure write permissions for the output location

## Troubleshooting

1. **Database Connection Issues**
   - Verify database credentials
   - Check if MySQL server is running
   - Ensure database and table exist

2. **Type Conversion Errors**
   - Check column types in the configuration
   - Verify value types in filter conditions
   - Use appropriate type conversion for dates and numbers

3. **File Not Found Errors**
   - Ensure config.json is in the correct location
   - Check output directory exists
   - Verify file paths in configuration

## Contributing

Feel free to submit issues and enhancement requests!