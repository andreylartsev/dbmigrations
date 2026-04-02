# Simple PostgreSQL database migrations tool

## Purpose of the tool:
- Ability to deploy a database from source code stored in a source code management system (Git);
- Streamline the delivery of changes in source code to test and production environments;

## Basic requirements:
- Delivery of changes in source code (database scripts/migrations) to the test environment;
- Delivery of changes in source code to the production environment;
- Ability to preserve data in existing databases (don't recreate db when you need apply some changes);
- Support the following types of changes:
  - Schema changes (tables, views, stored procedures)
  - Data changes;
- Ability to check which changes will be applied to a given database instance;
- Ability to generate an SQL script for use on a specific database for preliminary code review;
- Ability to use baseline scripts from the production environment (after anonymization, of course);

More you can find the [docs folder](./doc) 

## Why we need a tool?
- Why do you need a tool? Why not just embed the version control into the scripts themselves and deploy scripts them with a .bat/.ps/.sh file?
- A tool is necessary to simplify DDL/DML scripts themselves and remove from them the boilerplate code like the following: 
  - explicit transaction management; 
  - database version control code;
  - interception and logging of execution errors;
- Additional features should simplify the life: 
  - checking which scripts will be applied, generating DDL/DML scripts for DB administrator review.
- The defined process of db changes reduce the likelihood of shooting yourself in the foot.

In the end it is a sort of sample implementation that could be forked and customized for your needs

## Supported subcommands:
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

## How to create local python environment for installing dependencies:

You'll need python 3.11+ (mostly because of tomllib package using for parsing configuration) + psycopg package with its dependencies.  

All required packages are listed in requirements.txt
 
To create the local python environment and install required package you can try the following commands:

```  
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m venv .venv
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m pip install --upgrade pip
PS C:\Users\andrey.larcev\Projects\dbmigrations> python3.12.exe -m pip install requirements.txt
PS C:\Users\andrey.larcev\Projects\dbmigrations>  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
PS C:\Users\andrey.larcev\Projects\dbmigrations> .\.venv\Scripts\Activate.ps1
(.venv) PS C:\Users\andrey.larcev\Projects\dbmigrations>

```

## Sample DDL/DML scripts

The [samples folder](./dbmigrations/samples/) includes sample DML/DDL scripts repositories and you can try the tool with them:

- [test1](./dbmigrations/samples/test1) - included baseline, versioned and repeatable scripts
  - baseline/V000 - should create the table t1 and insert one record  
  - versions/V001 - should create the table t2 and insert one record 
  - repeatable/ - includes the script that should drop/create view on the table t1 
- [test1_empty_version](./dbmigrations/samples/test1_empty_version) - tests that empty versions are not allowed
- [test1_only_repeatable](./dbmigrations/samples/test1_only_repeatable) - shows that baseline and versioned scripts are not necessary if you specified target version in the target_version.txt file 
- and so on 
