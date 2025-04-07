""" Python Script that allows one to interact the popular open-source relational databases: 
MySQL, Postgres and Sqlite """

# Import the required modules

import time
import pprint
from typing import Optional
from sqlite3 import ProgrammingError as sqlite_error
from psycopg2 import ProgrammingError as postgres_error
from .constants import Constants



class SQLUtilities:
    """SQL Database Utilities Class"""


    @staticmethod
    def _get_cursor_type_name(cursor_object):
        """
        Returns the name of the cursor type corresponding to the provided cursor object.

        This method retrieves the type of the cursor object by examining its type and then 
        looks up the corresponding cursor name from the predefined `Constants.CURSOR_TYPES` 
        mapping. It ensures that the cursor object's type is valid by checking against 
        the predefined set of allowed types.

        Args:
            cursor_object (object): The cursor object whose type name is to be retrieved.

        Returns:
            str: The name of the cursor type corresponding to the given cursor object.

        Raises:
            AssertionError: If the cursor object's type is not found in the 
                            `Constants.CURSOR_TYPES` values.
        
        Example:
            cursor_object = some_cursor_instance
            cursor_type_name = _get_cursor_type_name(cursor_object)
            print(cursor_type_name)  # Prints the type name associated with the cursor object

        """

        # Directly check if cursor_object is one of the expected types
        cursor_type = str(type(cursor_object)).lower()
        cursor_type = cursor_type.split("'")[1]  # Extract the type name from the string

        # Ensure the cursor type is valid
        assert cursor_type in Constants.CURSOR_TYPES.values(), Constants.ASSERTION_ERROR_MESSAGE

        # Reverse lookup from CURSOR_TYPES to get the name of the cursor
        return cursor_type

    @staticmethod
    def select_all_query(table_name: str, cursor_object):
        """
        Get all results from the specified table.
        """
        SQLUtilities.execute_display_query_results(query=f"SELECT * FROM {table_name};",
                                                   cursor_object=cursor_object)

    @staticmethod
    def display_all_views_from_database(cursor_object) -> None:
        """
        Retrieves and displays all the views in the current database based on the type of database 
        (MySQL, PostgreSQL, or SQLite) using the provided cursor object.

        This function dynamically adjusts the query depending on the database type to show 
        the views in the active schema or database.

        Args:
            cursor_object (object): A database cursor object used to execute SQL queries.

        Returns:
            None: The function does not return any value. Instead, it directly displays the query 
                results using the `execute_display_query_results` method.

        Raises:
            Exception: If an unsupported database type is encountered or 
            if any query execution fails.
        
        Example:
            cursor_object = get_cursor_object()
            display_all_views_from_database(cursor_object)
            # Displays the views based on the cursor's database type
        """

        # Get the type of the cursor object (i.e., database type)
        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)

        # Base query to fetch views, customized later for different databases
        get_view_query: str = '''
        SELECT table_name, table_schema AS "DATABASE NAME", table_catalog
        FROM information_schema.tables WHERE TABLE_TYPE = "VIEW" 
        AND TABLE_SCHEMA = "{}"'''

        match cursor_type:
            case Constants.MYSQL:
                cursor_object.execute("select database()")
                db_name = cursor_object.fetchone()[0]
                SQLUtilities.execute_display_query_results(query=get_view_query.format(db_name),
                                                       cursor_object=cursor_object)
            case Constants.POSTGRES:
                db_name = SQLUtilities.__get_postgres_current_database(cursor_object)
                get_view_query = get_view_query + """ AND TABLE_SCHEMA NOT IN
                                                 ('information_schema', 'pg_catalog')"""
                SQLUtilities.execute_display_query_results(query=get_view_query.format(db_name),
                                                        cursor_object=cursor_object)
            case Constants.SQLITE:
                SQLUtilities.execute_display_query_results(
                    query="""SELECT name FROM sqlite_schema WHERE type ='view'
                    AND name NOT LIKE 'sqlite_%';""",
                    cursor_object=cursor_object)

    @staticmethod
    def show_databases(cursor_object) -> None:
        """
        Display the list of databases for the connected SQL server.

        This function determines the type of database from the given cursor object
        and executes the appropriate query to fetch and display the database names.

        Supported databases:
        - MySQL: Uses `SHOW DATABASES;`
        - PostgreSQL: Uses `SELECT datname FROM pg_database;`
        - SQLite: Uses `PRAGMA database_list;`

        Args:
            cursor_object: A database cursor object for executing queries.

        Raises:
            AssertionError: If the provided cursor is invalid.
        """
        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)


        query_map = {
            Constants.MYSQL: "SHOW DATABASES;",
            Constants.POSTGRES: "SELECT datname FROM pg_database;",
            Constants.SQLITE: "PRAGMA database_list;"
        }
        # Execute the appropriate query based on the cursor type
        # and display the results
        query = query_map.get(cursor_type, None)
        if query is None:
            raise ValueError(f"Unsupported cursor type: {cursor_type}")

        SQLUtilities.execute_display_query_results(query=query, cursor_object=cursor_object)

    @staticmethod
    def display_all_tables_in_database(cursor_object, database_name: str = None) -> None:
        """
        Displays all tables in the specified database.

        This function determines the type of database from the given cursor object
        and executes the appropriate query to fetch and display table names.

        Supported databases:
        - MySQL: Uses `information_schema.tables`
        - PostgreSQL: Uses `information_schema.tables`
        - SQLite: Uses `sqlite_schema`

        Args:
            cursor_object: A database cursor object used to execute SQL queries.
            database_name (str, optional): Name of the database to show tables from.
                                        If None, it defaults to the current database.

        Raises:
            AssertionError: If the provided cursor is invalid.
        """
        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)

        match cursor_type:
            case Constants.MYSQL:
                database_name = database_name if database_name else \
                SQLUtilities.__get_mysql_current_database(cursor_object)

                query_str = f'''SELECT table_name, table_schema AS "DATABASE NAME", table_catalog
                                FROM information_schema.tables WHERE TABLE_TYPE = "BASE TABLE" 
                                AND TABLE_SCHEMA = "{database_name}"'''

            case Constants.POSTGRES:
                # For PostgreSQL, get the current database if not provided
                database_name = database_name or \
                    SQLUtilities.__get_postgres_current_database(cursor_object)
                query_str = f"""SELECT table_name, table_schema
                                FROM information_schema.tables 
                                WHERE table_catalog = '{database_name}'
                                AND table_schema NOT IN ('pg_catalog', 'information_schema');"""

            case Constants.SQLITE:
                # For SQLite, use the sqlite_schema
                query_str = """SELECT name FROM sqlite_schema WHERE type = 'table'
                               AND name NOT LIKE 'sqlite_%';"""

        SQLUtilities.execute_display_query_results(query=query_str, cursor_object=cursor_object)

    @staticmethod
    def summary_statistics(table_name: str, cursor_object: object,
                           column_names: list = None,) -> None:
        """
        Display summary statistics (Count, Min, Max, Avg, and Sum) for 
        all numeric columns in a table.

        This function determines the database type from the provided cursor object and
        executes the appropriate summary statistics processing method.

        Supported Databases:
        - MySQL
        - PostgreSQL
        - SQLite

        Args:
            table_name (str): The name of the table for which statistics are generated.
            cursor_object (object): A database cursor object used for executing SQL queries.

        Raises:
            ValueError: If the table name is empty or invalid.
            AssertionError: If the provided cursor object is not valid.
        """
        if not table_name.strip():
            raise ValueError("Invalid table name. Please provide a non-empty table name.")

        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)


        # Map cursor type to corresponding processing function
        db_processors = {
            Constants.MYSQL: SQLUtilities.__process_mysql_summary_stats,
            Constants.POSTGRES: SQLUtilities.__process_psycopg2_summary_stats,
            Constants.SQLITE: SQLUtilities.__process_sqlite_summary_stats
        }

        # Get the appropriate processor or fall back to a default (if needed)
        processor = db_processors.get(cursor_type, None)
        if processor is None:
            raise ValueError(f"Unsupported cursor type: {cursor_type}")
        processor(table_name, cursor_object, column_names)

    @staticmethod
    def __process_mysql_summary_stats(table_name: str, cursor_object: object,
                                      column_names: list = None) -> None:
        """
        Processes and displays summary statistics for numeric and date columns in a MySQL table.
        
        This function retrieves the columns from the specified table, and for each column:
        - If the column is numeric, it fetches the count, maximum, minimum, average, and sum.
        - If the column is of a date type, it fetches the count, maximum, minimum, and sum.
        
        It skips columns with '_id' in the name, primary key columns, and columns not in
        `column_names` if specified.

        Args:
            table_name (str): The name of the table to process.
            cursor_object (object): The MySQL cursor object used to execute queries.
            column_names (list, optional): A list of specific column names to process. 
            If `None`, all columns are processed.

        Returns:
            None
        """
        # Fetch the column details from the table
        cursor_object.execute(f"SHOW COLUMNS FROM {table_name};")
        results = cursor_object.fetchall()

        print(Constants.SUMMARY_MESSAGE.format(Constants.DASHES, table_name, Constants.DASHES))

        for column_name, data_type, _, column_key, _, _ in results:
            # Skip columns with '_id' in the name
            # Skip columns not in the specified column names list, if provided
            if '_id' in column_name or (column_names and column_name not in column_names):
                continue

            column_type = data_type.lower()

            # Process numeric columns
            if column_type in Constants.NUMERIC_TYPES or 'decimal' in column_type:
                if column_key in {'PRI', 'MUL'}:
                    continue
                query = f"""
                    SELECT COUNT(*), MAX({column_name}), MIN({column_name}),
                        AVG({column_name}), SUM({column_name})
                    FROM {table_name};
                """
                SQLUtilities.execute_display_query_results(query=query, cursor_object=cursor_object)

            # Process date columns
            elif column_type in Constants.DATE_TYPES:
                if column_key in {'PRI', 'MUL'}:
                    continue
                query = f"""
                    SELECT COUNT(*), MAX({column_name}), MIN({column_name})
                    FROM {table_name};
                """
                SQLUtilities.execute_display_query_results(query=query, cursor_object=cursor_object)

    @staticmethod
    def __get_postgres_current_database(postgres_cursor: object) -> str:
        """Fetches the current database for PostgreSQL"""
        postgres_cursor.execute("select current_database()")
        db_name = postgres_cursor.fetchone()[0]
        return db_name

    @staticmethod
    def __get_mysql_current_database(mysql_cursor) -> str:
        """Fetches the current database for MySQL"""
        mysql_cursor.execute("SELECT DATABASE()")
        db_name = mysql_cursor.fetchone()[0]
        return db_name

    @staticmethod
    def __get_postgres_table_schema(table_name: str, postgres_cursor: object) -> str:
        current_database = SQLUtilities.__get_postgres_current_database(postgres_cursor)
        query_str = f"""SELECT table_schema FROM
        information_schema.tables WHERE table_catalog = '{current_database}'
        AND table_name = '{table_name}'
        AND table_schema NOT IN ('pg_catalog', 'information_schema');"""
        postgres_cursor.execute(query_str)
        results = postgres_cursor.fetchall()
        return results[0][0]

    @staticmethod
    def __process_psycopg2_summary_stats(table_name: str, cursor_object: object,
                                         column_names: list = None) -> None:
        table_schema = SQLUtilities.__get_postgres_table_schema(table_name, cursor_object)
        cursor_object.execute(f"""SELECT cols.column_name, data_type,
                            CASE WHEN pk.column_name = cols.column_name
                            THEN 'YES' ELSE 'NO' END AS primary_key
                            FROM information_schema.columns AS cols
                            LEFT JOIN (SELECT column_name 
                            FROM information_schema.table_constraints AS tc
                            JOIN information_schema.key_column_usage
                            USING (table_name) WHERE constraint_type = 'PRIMARY KEY'
                            AND tc.table_schema = '{table_schema}'
                            AND tc.table_name = '{table_name}'
                            ORDER BY ordinal_position) AS pk
                            ON cols.column_name = pk.column_name
                            WHERE cols.table_name= '{table_name}';""")
        results = cursor_object.fetchall()
        print(Constants.SUMMARY_MESSAGE.format(Constants.DASHES, table_name,
                                                    Constants.DASHES))
        for column_name, data_type, is_primary_key in results:
            if '_id' in column_name or data_type not in Constants.NUMERIC_TYPES or \
            is_primary_key == "YES":
                continue
            if column_names and column_name not in column_names:
                continue
            SQLUtilities.execute_display_query_results(query=f"""SELECT COUNT(*),
                                                        MAX({column_name}) AS MAX_{column_name},
                                                        MIN({column_name}) AS MIN_{column_name},
                                                        ROUND(AVG({column_name}), 4)
                                                        AS AVG_{column_name},
                                                        SUM({column_name}) AS SUM_{column_name}
                                                        FROM {table_schema}.{table_name};""",
                                                        cursor_object=cursor_object)

    @staticmethod
    def __process_sqlite_summary_stats(table_name: str, cursor_object: object,
                                      numeric_types: list) -> None:
        cursor_object.execute(f"PRAGMA table_info({table_name});")
        results = cursor_object.fetchall()
        print(Constants.SUMMARY_MESSAGE.format(Constants.DASHES, table_name,
                                                    Constants.DASHES))
        for result in results:
            if result[5] == 1:
                continue
            num_type: str = result[2].lower()
            if num_type in numeric_types or 'decimal' in num_type:
                column_name: str = result[1]
                SQLUtilities.execute_display_query_results(query=f"""SELECT COUNT(*),
                                                           MAX({column_name}),
                                                           MIN({column_name}), AVG({column_name}),
                                                           SUM({column_name}) FROM {table_name};""",
                                                           cursor_object=cursor_object)

    @staticmethod
    def get_create_table_statement(table_name: str, cursor_object: object) -> None:
        """
        Retrieves and prints the CREATE TABLE statement for a given MySQL table,
        modifying it to include "IF NOT EXISTS" for safer execution.
        
        This method only supports MySQL cursors.
        
        Args:
            table_name (str): The name of the table to retrieve the CREATE TABLE query for.
            cursor_object (mysql.connector.cursor.MySQLCursor): A MySQL database cursor used 
            to execute SQL queries.
        
        Returns:
            None: This function does not return a value; 
            it prints the CREATE TABLE statement directly.
        
        Raises:
            ValueError: If the table_name is empty.
            AssertionError: If the cursor_object is invalid.
        """
        if not table_name:
            raise ValueError(f"Table '{table_name}' does not exist or could not retrieve schema.")

        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)
        assert Constants.MYSQL == cursor_type, Constants.ASSERTION_MYSQL_ERROR_MESSAGE


        cursor_object.execute(f"SHOW CREATE TABLE `{table_name}`;")
        result = cursor_object.fetchone()

        if result:
            query = result[1].replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            pprint.pprint(query)

    @staticmethod
    def find_substr_index_in_string(substr: str, string: str, cursor_object: object) -> None:
        """
        Find the index of a substring in a string.

        Args:
            substr (str): The substring to search for.
            string (str): The string to search within.

        Returns:
            int: The index of the first occurrence of the substring in the string.
        """
        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)

        if Constants.MYSQL in cursor_type:
            SQLUtilities.execute_display_query_results(
            query=f"SELECT LOCATE('{substr}', '{string}');", cursor_object=cursor_object)
        elif Constants.POSTGRES in cursor_type:
            SQLUtilities.execute_display_query_results(
            query=f"SELECT POSITION('{substr}' IN '{string}');", cursor_object=cursor_object)
        else:
            SQLUtilities.execute_display_query_results(
            query=f"SELECT INSTR('{string}', '{substr}');", cursor_object=cursor_object)

    @staticmethod
    def show_columns(table_name: str, cursor_object: object) -> None:
        """
        Display all columns for a given table, including metadata such as data type, 
        nullability, default values, and primary key status (if applicable).

        Supported Databases:
        - MySQL
        - PostgreSQL
        - SQLite

        Args:
            table_name (str): The name of the table whose columns are to be displayed.
            cursor_object (object): A database cursor object used to execute SQL queries.

        Raises:
            ValueError: If the table name is empty or invalid.
            AssertionError: If the provided cursor object is not valid.
        """
        if not table_name.strip():
            raise ValueError(Constants.INVALID_TABLE_ARGUMENT)
        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)

        # Define SQL queries for different databases
        query_map = {
            Constants.MYSQL: f"SHOW COLUMNS FROM {table_name};",
            Constants.SQLITE: f"PRAGMA table_info({table_name});"
        }

        if Constants.POSTGRES in cursor_type:
            # Get the schema of the table
            # This is necessary for PostgreSQL to identify the correct schema
            table_schema = SQLUtilities.__get_postgres_table_schema(table_name, cursor_object)
            query_map[Constants.POSTGRES] = f"""
                SELECT c.column_name, c.data_type, c.is_nullable, c.is_identity, c.column_default,
                    CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS primary_key
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.column_name 
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = '{table_schema}'
                    AND tc.table_name = '{table_name}'
                ) pk
                ON c.column_name = pk.column_name
                WHERE c.table_name = '{table_name}';
            """

        query = query_map.get(cursor_type)

        if query:
            SQLUtilities.execute_display_query_results(query=query, cursor_object=cursor_object)
        else:
            raise ValueError(f"Unsupported database type: {cursor_type}")
        
    @staticmethod
    def execute_stored_procedure(procedure_name: str, parameters: tuple, cursor_object: object) -> None:
        """
        Executes a stored procedure using the given cursor and displays the result set.

        Args:
            procedure_name (str): Name of the stored procedure to execute.
            parameters (tuple): Tuple of parameters to pass to the stored procedure.
                                Pass an empty tuple or None if no parameters are needed.
            cursor_object (object): Database cursor object capable of executing stored procedures.

        Raises:
            ValueError: If `parameters` is not a tuple and is not None.
        """
        print(f"Calling the Procedure '{procedure_name}' with parameters {parameters}")
        start_time = time.perf_counter()

        if parameters is None:
            parameters = ()

        if not isinstance(parameters, tuple):
            raise ValueError("Parameters should be passed as a tuple.")
        
        cursor_object.callproc(procedure_name, parameters)
        execution_time = round(time.perf_counter() - start_time, 3)

        results=next(cursor_object.stored_results())
        SQLUtilities.__display_results(
            table_column_names=results.column_names,
            results=results.fetchall(),
            exec_time=execution_time,
            result_limit=50
        )

    @staticmethod
    def database_exists(database_name: str, cursor_object: object) -> bool:
        """
        Check if a given database exists in the connected database system.

        Supported Databases:
        - MySQL
        - PostgreSQL
        - SQLite

        Args:
            database_name (str): The name of the database to check.
            cursor_object (object): A database cursor object used to execute SQL queries.

        Returns:
            bool: True if the database exists, otherwise False.

        Raises:
            ValueError: If the provided database name is empty or invalid.
            AssertionError: If the provided cursor object is not valid.
        """
        if not database_name.strip():
            raise ValueError("Database name cannot be empty or only spaces.")

        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)

        # Define database existence check queries
        query_map = {
            Constants.MYSQL: "SHOW DATABASES;",
            Constants.SQLITE: "PRAGMA database_list;",
            Constants.POSTGRES: f"""SELECT 1 FROM pg_catalog.pg_database
            WHERE datname = '{database_name}';"""
        }
        query = query_map.get(cursor_type)
        if not query:
            print(f"Unsupported database type: {cursor_type}")
            return False  # Unsupported database type
        cursor_object.execute(query)

        match cursor_type:
            case Constants.MYSQL:
                return database_name in [db[0] for db in cursor_object.fetchall()]
            case Constants.SQLITE:
                return any(database_name in db[1] for db in cursor_object.fetchall())
            case Constants.POSTGRES:
                return cursor_object.fetchone() is not None


    @staticmethod
    def __print_plus_dashes(table_columns_length: list) -> None:
        """  
        Prints the plus and dashes  
        """
        plus_dashes = (
            "+" + "+".join("-" * (length + 2) for length in table_columns_length) + "+"
        )
        print(plus_dashes)

    @staticmethod
    def __print_table_headers(table_columns_length: list, table_column_names: list[str]) -> None:
        """  
        Prints the table headers  
        """
        SQLUtilities.__print_plus_dashes(table_columns_length)

        # Print the table headers
        table_headers = (
            "|"
            + "|".join(
                f" {name:^{table_columns_length[i]}} "
                for i, name in enumerate(table_column_names)
            )
            + "|"
        )
        print(table_headers)
        SQLUtilities.__print_plus_dashes(table_columns_length)

    @staticmethod
    def __display_results(
        table_column_names: list[str],
        results: list[list],
        exec_time: float,
        result_limit,
    ) -> None:
        """
        Displays the results of a query in a formatted table.

        Args:
        table_column_names (list[str]): The names of the columns in the result set.
        results (list[list]): The rows of data returned from the query.
        exec_time (float): The time taken to execute the query.
        result_limit (int, optional): The maximum number of results to display. Defaults to 10.

        Returns:
        None
        """
        table_columns_length = [len(name) for name in table_column_names]

        # Calculate the maximum length of each column based on the results
        for result in results:
            for index, row_data in enumerate(result):
                row_data_str = str(row_data) if row_data is not None else 'NULL'
                table_columns_length[index] = max(table_columns_length[index], len(row_data_str))

        SQLUtilities.__print_table_headers(table_columns_length, table_column_names)

        # Print the table rows
        is_truncated: bool = False
        is_same: bool = (
            len(results) == result_limit
        )  # Check if the len of results returned is the same as the result limit
        for limit, result in enumerate(results):
            table_row = (
                "|"
                + "|".join(
                    f""" {str(row_data) if row_data is not None
                    else 'NULL':^{table_columns_length[i]}} """
                    for i, row_data in enumerate(result)
                )
                + "|"
            )
            print(table_row)
            if limit + 1 == result_limit:
                is_truncated = True
                break

        SQLUtilities.__print_plus_dashes(table_columns_length)
        if is_truncated and not is_same:
            print(f"!!!Result Truncated. Showing only {result_limit} results!!!")
        message = "row returned" if len(results) == 1 else "rows returned"
        print(f"{len(results)} {message} in time: ({exec_time} sec)")
        print("\n")

    @staticmethod
    def execute_query(query: str, cursor_object: object) -> None:
        """ Executes the passed query"""
        start_time = time.perf_counter()
        exec_time: int = 0
        try:
            cursor_object.execute(query)
            exec_time = time.perf_counter() - start_time
            exec_time = round(exec_time, 3)
            print(f"Query ran successfully in time: ({exec_time} sec)")
        except (sqlite_error, postgres_error, SyntaxError) as error:
            print(f"An error occurred: {error}")
            raise


    @staticmethod
    def execute_display_query_results(
        query: str,
        cursor_object: object,
        logger: Optional[object] = None
    ) -> None:
        """
        Executes a SQL query and displays the results in a formatted table.

        Args:
            query (str): The SQL query to be executed.
            cursor_object: The database cursor object used to execute the query.
            logger (Optional[object], optional): A logger object for logging query execution.
            Defaults to None.

        Returns:
            None: This function does not return a value; it prints the results directly.
        """
        cursor_type = SQLUtilities._get_cursor_type_name(cursor_object)

        result_limit: int = 50
        if logger:
            logger.info(f"Executing the query: {query}")

        query_string: str = query.lower()
        if "limit" in query_string:
            index_of_limit: int = query_string.index("limit")
            result_limit = query_string[index_of_limit + 6:]
        start_time = time.perf_counter()
        exec_time: int = 0
        try:
            cursor_object.execute(query)
            exec_time = time.perf_counter() - start_time
            exec_time = round(exec_time, 3)
            results = cursor_object.fetchall()
        except (sqlite_error, postgres_error, SyntaxError) as error:
            print(f"An error occurred: {error}")
            raise error
        if Constants.MYSQL == cursor_type:
            table_column_names = cursor_object.column_names
        else:
            table_column_names = [
                description[0] for description in cursor_object.description]
        SQLUtilities.__display_results(
            table_column_names, results, exec_time, result_limit
        )
