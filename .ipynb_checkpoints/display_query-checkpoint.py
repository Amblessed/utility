from time import time
from typing import Optional

def select_all_query(table_name: str):
    query = f"""SELECT * FROM {table_name} LIMIT 25;"""
    return query

def is_table_created_and_empty(table_name: str, cursor_object) -> tuple[bool, int]:
    cursor_object.execute("SHOW TABLES;")
    tables: list = [table[0] for table in cursor_object] 
    
    cursor_object.execute(f"SELECT COUNT(*) FROM {table_name};")
    # Get the result of the query
    result = mysql_cursor.fetchone()
    # The result is a tuple with one element, which contains the count
    row_count = result[0]
    return (table_name in tables, row_count)

def display_results(table_column_names: list[str], results: list[list], exec_time: float, result_limit) -> None:
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
            if row_data is not None:
                row_data_str = str(row_data)
                table_columns_length[index] = max(table_columns_length[index], len(row_data_str))

    # Create the top border of the table
    plus_dashes = "+" + "+".join('-' * (length + 2) for length in table_columns_length) + "+"
    print(plus_dashes)

    # Print the table headers
    table_headers = "|" + "|".join(f" {name:^{table_columns_length[i]}} " for i, name in enumerate(table_column_names)) + "|"
    print(table_headers)
    print(plus_dashes)

    # Print the table rows
    isTruncated: bool = False
    isSame = len(results) == result_limit  # Check if the len of results returned is not the same as the result limit
    for limit, result in enumerate(results):
        table_row = "|" + "|".join(f" {str(row_data) if row_data is not None else 'NULL':^{table_columns_length[i]}} " for i, row_data in enumerate(result)) + "|"
        print(table_row)
        if limit + 1 == result_limit:
            isTruncated = True
            break

    print(plus_dashes)
    if isTruncated and not isSame:
        print(f"!!!Result Truncated. Showing only {result_limit} results!!!")
    message = "row returned" if len(results) == 1 else "rows returned"
    print(f"{len(results)} {message} in time: ({exec_time} sec)")
    print("\n")



def execute_display_query_results(query: str, cursor_object, logger: Optional[object] = None, result_limit: int = 50) -> None:
    """
    Executes a SQL query and displays the results in a formatted table.

    Args:
        query (str): The SQL query to be executed.
        cursor_object: The database cursor object used to execute the query.
        logger (Optional[object], optional): A logger object for logging query execution. Defaults to None.

    Returns:
        None: This function does not return a value; it prints the results directly.
    """
    if logger:
        logger.info(f"Executing the query: {query}")

    
    query_string: str = query.lower()
    if "limit" in query_string:
        index_of_limit: int = query_string.index("limit")
        result_limit = query_string[index_of_limit+6: ]
    init_time = time()
    cursor_object.execute(query)
    exec_time = time() - init_time
    results = cursor_object.fetchall()
    table_column_names = cursor_object.column_names  
    display_results(table_column_names, results, round(exec_time, 3), result_limit)



"""
def display_results(table_column_names: list, results: list, exec_time: time):
    result_limit: int = 100
    isLimited: bool = False
    num_rows: int = len(results)
    table_columns_length = [len(x) for x in table_column_names]
    for result in results:
        for value in range(len(result)):
            row_data = result[value]
            if row_data:
                row_data = str(row_data)
                if len(row_data) > table_columns_length[value]:
                    table_columns_length[value] = len(row_data)
    plus_dashes = ""
    for num in range(len(table_columns_length)):
        plus_dashes = plus_dashes + "+" + '-'*(table_columns_length[num]+2)
    plus_dashes = plus_dashes + "+"
    
    print(plus_dashes)
    
    table_headers = ""
    for num in range(len(table_column_names)):
        table_headers = table_headers + f"| {table_column_names[num]:^{table_columns_length[num]}} "
    table_headers = table_headers + "|"
    print(table_headers)
    
    print(plus_dashes)
    limit: int = 0
    for result in results:
        table_row = ""
        for value in range(len(result)):
            row_data = result[value]
            if row_data is None:
                row_data = "NULL"            
            table_row = table_row + "|" + f"{str(row_data):^{table_columns_length[value]+2}}"
        print(table_row + "|")
        limit = limit + 1
        if limit == result_limit:
            isLimited = True
            break
    print(plus_dashes)
    if isLimited:
        print(f"!!!Result Truncated. Showing only {result_limit} results!!!")
    message: str = "row returned" if num_rows == 1 else "rows returned"
    print(f"{num_rows} {message} in time: ({exec_time} sec)")
    print("\n")

def execute_display_query_results(query: str, cursor_object, logger = None):
    if logger:
        logger.info(f"Executing the query: {query}")
    init_time = time()
    cursor_object.execute(query)
    end_time = time()
    exec_time = end_time - init_time
    results = cursor_object.fetchall()    
    table_column_names = cursor_object.column_names  
    display_results(table_column_names, results, round(exec_time, 3))
"""