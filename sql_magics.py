from functools import wraps
from typing import Any, Callable, TypeVar, cast
import sqlparse
import pandas as pd
import psycopg2
from IPython import get_ipython
from IPython.core.magic import Magics, cell_magic, magics_class


QUERY_LIMIT = 1000
FuncT = TypeVar("FuncT", bound=Callable[..., list])


def query_handler(func: FuncT) -> FuncT:
    """"Query handler to handle exceptions or interruptions during the query execution"""
    @wraps(func)
    def wrapper_profiler(*args: Any, **kwargs: Any) -> list:
        result = []
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            print(f"Error: {e}")
        except KeyboardInterrupt:
            print("Aborted.")
        return result
    return cast(FuncT, wrapper_profiler)


class NoConnectionSettingsException(Exception):
    pass


def get_postgresql_connection(url: str) -> psycopg2.extensions.connection:

    data = url.split(sep='//')[1]
    user = data.split(':')[0]
    password, host = data.split(':')[1].split('@')
    port, database = data.split(':')[2].split('/')

    if "postgres" not in url.split(sep='//')[0]:
        raise NoConnectionSettingsException(
            f"Sorry, your Postgres connection have not been configured yet\n"
        )

    return psycopg2.connect(
        dbname=database,
        host=host,
        port=port,
        user=user,
        password=password
    )


@query_handler
def execute_sql(
    cursor: psycopg2.extensions.cursor,
    statement: str,
    fetch_size: int = QUERY_LIMIT,
) -> pd.DataFrame:
    if not isinstance(cursor, psycopg2.extensions.cursor):
        raise NoConnectionSettingsException(
            f"Sorry, but you have to specify psycopg2.extensions.cursor first\n"
        )
    else:
        cursor.execute(statement)
    result = cursor.fetchmany(fetch_size)
    df = pd.DataFrame(result)
    if df.shape[0]:
        df.columns = [desc[0] for desc in cursor.description]
    return df


def format_sql(query: str) -> str:
    return sqlparse.format(
        query,
        keyword_case="upper",
        identifier_case="lower",
        comma_first=True,
        strip_comments=False,
        #reindent_aligned=True,
        #reindent=True,
    )


def format_cell(line: str, cell: str) -> str:
    formatted_sql = format_sql(cell)
    if line.strip() == "pgsql":
        magic = "pgsql <YOUR POSTGRES CONNECTION STRING>"
    elif line.strip() == "engine":
        magic = "time"
        formatted_sql = f'df = pd.read_sql("""\n{formatted_sql}""", engine)'
    else:
        magic = " ".join(line.split()) if line else "time"
    return f"%%{magic}\n{formatted_sql}"


@magics_class
class SQLMagics(Magics):

    @cast(Callable, cell_magic)
    def pgsql(self, line: str, cell: str) -> pd.DataFrame:
        with get_postgresql_connection(url=line) as conn, conn.cursor() as cursor:
            return execute_sql(cursor, cell)

    @cast(Callable, cell_magic)
    def format(self, line: str, cell: str) -> pd.DataFrame:
        get_ipython().set_next_input(format_cell(line, cell), replace=True)


get_ipython().register_magics(SQLMagics)
print("%%format magic is ready to be used to make your SQL better")
print("Run `%%pg_sql database_connection_url` to obtain pre-configured connection to PostgreSQL")