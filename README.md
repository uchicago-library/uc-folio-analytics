# uc-folio-analytics
FOLIO reports and other analytic queries


# Coding conventions
Use consistent coding conventions, including automatic code formatting, to have a consisten style for easier reading and less noise code diffs.

## SQL Conventions

  * Keywords: uppercase
  * Types: lowercase
  * Indent: 4 spaces
  * Tabs: use tabs instead of spaces

## End-of-line Conventions

Use UNIX EOL conventions. Most IDEs can be configured for this.
  
## SQL Formatting

For consistent formatting of SQL source, use [pgFormatter](https://sqlformat.darold.net) with default settings. pgFormatter is available two ways:

* `pg_format` commandline utility, available from [https://github.com/darold/pgFormatter](https://github.com/darold/pgFormatter)
* [pgFormatter](https://sqlformat.darold.net) demo website

To run `pg_format` to format a file of SQL and format in place:
```
pg_format --inplace [filename]
```
`pg_format` can be integrated with most SQL IDEs or run separately on the command line.


