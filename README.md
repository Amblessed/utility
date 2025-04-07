# SQLUtilities
SQLUtilities is a Python utility class designed to interact with SQL databases like MySQL, PostgreSQL, and SQLite. It simplifies common SQL database operations such as retrieving all tables, views, and databases, as well as dynamically adjusting to different SQL database types.

## Features
**Dynamic Cursor Type Handling**: Automatically detects the type of database (MySQL, PostgreSQL, SQLite) based on the provided cursor object and adapts queries accordingly.

**Display Tables and Views**: Fetches and displays all tables and views in the current database for supported SQL databases.

**Show Databases**: Displays the list of available databases for the connected SQL server.

**Cross-Database Compatibility**: Supports MySQL, PostgreSQL, and SQLite with appropriate queries tailored for each database type.
