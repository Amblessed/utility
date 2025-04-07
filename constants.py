""" Constants for the script """


class Constants:
    """ Class for the constant values """
    NUMERIC_TYPES: list = ["tinyint", "smallint", "mediumint", "int", "int2",
                            "int8", "integer", "bigint", "float", "double",
                            "decimal", "numeric", "real", "double precision", 
                            "smallserial", "serial", "bigserial"]
    DATE_TYPES: list = ["date", "datetime", "timestamp"]
    # str(type(cursor_object)): this returns one of the following depending
    # on the cursor object passsed
    # <class 'mysql.connector.cursor_cext.cmysqlcursor'>
    # <class 'sqlite3.cursor'>
    # <class 'psycopg2.extensions.cursor'>
    #EXCEPTION_TYPES: list = ["SyntaxError", "DuplicateDatabase",
                            #"sqlite3.cursor"]

    ASSERTION_ERROR_MESSAGE: str = "Unsupported cursor type: {}"
    ASSERTION_MYSQL_ERROR_MESSAGE: str = "Please pass a mysql cursor object"
    DASHES: str = "===================="
    SUMMARY_MESSAGE: str = "{} SUMMARY STATISTICS FOR {} TABLE {}"
    MYSQL: str = "mysql.connector.cursor_cext.cmysqlcursor"
    POSTGRES: str = "psycopg2.extensions.cursor"
    SQLITE: str = "sqlite3.cursor"
    INVALID_TABLE_ARGUMENT: str = "Please pass a valid table name!!!!"
    CURSOR_TYPES: dict = {POSTGRES: "psycopg2.extensions.cursor",
                          MYSQL:"mysql.connector.cursor_cext.cmysqlcursor",
                          SQLITE:"sqlite3.cursor"}
