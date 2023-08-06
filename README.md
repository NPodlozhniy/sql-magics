# sql-magics
Useful magic tools to facilitate SQL writing and execution within Jupyter

## Quick Start

 - Use `%%format` to reformat the sql, in addition you can change the rules in the function [format_sql](https://github.com/NPodlozhniy/sql-magics/blob/e8ab1b270593f105ce457b8d47ce2de62c41f998/sql_magics.py#L73)
 - Use `%%format` or `%%pgsql` with the connection string to execute the queries in place
 - Use `%%format engine` to put the result of the query to dataframe if you using `sqlalchemy` engine

## Details
 - Take a look at the demo notebook [testing.ipynb](https://github.com/NPodlozhniy/sql-magics/blob/master/testing.ipynb)
