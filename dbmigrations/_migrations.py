from __future__ import annotations

from abc import ABC, abstractmethod

from _errors import CommandError
from _i18n import _



class OwnMigration(ABC):    
    @abstractmethod
    def get_sql_to_check_if_need_migration(self):
        pass
    @abstractmethod
    def get_migration_ddl(self):
        pass
    @abstractmethod
    def get_migration_desc(self):
        pass

class MigrationCheckForOlderVersionControlTables (OwnMigration):
    def get_sql_to_check_if_need_migration(self) -> str:
        sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {schema_name_str} AND table_name = 'dbmigration_versions'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = {schema_name_str} AND table_name = 'dbmigration_version_scripts'
            ) AS conditions_met;     
        """
        return sql
    def get_migration_ddl(self) -> str:
        raise CommandError(
            _(
                "This version of dbmigration tools is incompatible with this schema.\n"
                "Please use the previous version available by tag 0.9.x or upgrade the current schema by "
                "deleting dbmigration_version and dbmigration_repeatable tables and running the update "
                "subcommand with --force-run-cleanup flag: \n"
                "i.e. dbmigration.py update <schema_name> <scripts_folder> --force-run-cleanup"
            )
        )
                  
    def get_migration_desc(self) -> str:
        desc = _("Check for older version control tables")
        return desc
