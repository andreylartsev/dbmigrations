# Simple database migrations tool


```
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations> python3 .\dbmigrations\dbmigration.py
usage: dbmigration.py [-h] {update,verify,init} ...

Simple database migrations tool

positional arguments:
  {update,verify,init}  Available subcommands
    update              Applies base, versioned, and repeatable scripts to the target database schema.
    verify              Validates the target schema and lists versioned and reproducible scripts to apply if the
                        'update' command is executed.
    init                Creates version control tables in an empty database schema.

options:
  -h, --help            show this help message and exit
```

## How to create local python environment for installing dependencies

All required packages are listed in requirements.txt
 
Create the local python environment and install required package

```  
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m venv .venv
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m pip install --upgrade pip
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m pip install requirements.txt
PS C:\Users\andrey.larcev\Projects\dbmigrations>  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
PS C:\Users\andrey.larcev\Projects\dbmigrations> .\.venv\Scripts\Activate.ps1
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations>

```

