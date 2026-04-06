# Sample DDL/DML scripts

This folder includes sample DML/DDL scripts repositories and you can try the tool with them:

- [test1](./test1) - In this folder included baseline, versioned and repeatable scripts:
  - baseline/V000 - should create the table t1 and insert one record;  
  - versions/V001 - should create the table t2 and insert one record; 
  - repeatable/ - includes the script that should drop/create view on the table t1; 
- [test1_empty_baseline_and_repeatable](./test1_empty_baseline_and_repeatable) - The dummy baseline script + repeatable script(s). Demonstration of extending other`s system schema i.e. integration via DB. Yes, well known anti-pattern;
- [test1_empty_version](./test1_empty_version) - Tests that "empty" versions are not allowed;
- [test1_just_baseline](./test1_just_baseline) - This checks that the folder can only contain basic scripts;
- [test1_latest_version_only_and_repeatable](./test1_latest_version_only_and_repeatable) - this checks that the folder can only contain just one latest version and repeatable scripts;
- [test1_no_repeatable](./test1_no_repeatable) - this checks that the folder can only contain baseline andd versioned scripts;
- [test1_only_repeatable](./test1_only_repeatable) - This shows that baseline and versioned scripts are not necessary if you specified target version in the target_version.t
- [test1_with_lists](./test1_with_lists) - Demonstration of usage 'script_list.txt' file to define the order of script execution instead of relaying on the sorting scripts by its name in alphabetical order; The requested feature.
- [test1_with_wrong_version_following](./test1_with_wrong_version_following) - Demonstration of cross-check of right order of the baseline version and the versioned scripts version

[See also](../../README.md)