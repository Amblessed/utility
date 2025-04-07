# SQLUtilities
SQLUtilities is a Python utility class designed to interact with SQL databases like MySQL, PostgreSQL, and SQLite. It simplifies common SQL database operations such as retrieving all tables, views, and databases, as well as dynamically adjusting to different SQL database types.

## Features
**Dynamic Cursor Type Handling**: Automatically detects the type of database (MySQL, PostgreSQL, SQLite) based on the provided cursor object and adapts queries accordingly.

**Display Tables and Views**: Fetches and displays all tables and views in the current database for supported SQL databases.

**Show Databases**: Displays the list of available databases for the connected SQL server.

**Cross-Database Compatibility**: Supports MySQL, PostgreSQL, and SQLite with appropriate queries tailored for each database type.

# Installation
git clone https://github.com/Amblessed/SQLUtilities.git

# Requirements
- Python 3.x
- Database connectors (MySQL, PostgreSQL, or SQLite)
  - For MySQL: pip install mysql-connector-python
  - For PostgreSQL: pip install psycopg2
  - For SQLite: Built-in with Python (no additional installation needed)
 
# Usage
## Importing the Class
You can import the SQLUtilities class into your Python project as follows:  
```python from sql_utilities import SQLUtilities```

# 1. Get All Results from a Table
To get all rows from a specific table:  
```SQLUtilities.select_all_query(table_name="your_table_name", cursor_object=your_cursor)```
