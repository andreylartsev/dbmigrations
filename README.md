```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> .\.venv\Scripts\python.exe .\dbmigrations\dbmigration.py
usage: dbmigration.py [-h] {update,verify,init} ...

Simple database migrations tool

positional arguments:
  {update,verify,init}  Available subcommands
    update              Applies baseline, versioned and repeatable scripts within target database schema
    verify              Verifies target schema and lists versioned and repeatable scripts to be applied within
    init                Creates version control tables in the empty database schema

options:
  -h, --help            show this help message and exit
```
