"""UpdateCommand: applies pending migration scripts."""

from pathlib import Path

from _constants import (
    BASELINE_DIR_NAME,
    BASELINE_FILES_DEPTH,
    REPEATABLE_DIR_NAME,
    REPEATABLE_FILES_DEPTH,
    TARGET_VERSION_FILE,
    VERSIONED_DIR_NAME,
    VERSIONED_FILES_DEPTH,
)
from _confirmation import Confirmation
from _errors import CommandError
from _i18n import _
from _options import Deps, UpdateOptions
from _output import Output
from _scripts import ScriptFsInfo, read_as_trimmed_string
from _tool import ExternalTool

from commands.base import BaseCommand


class UpdateCommand (BaseCommand):
    """Applies base, versioned, and repeatable scripts to the target database schema."""

    def run_baseline_scripts_with_external_tool(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path], 
        tool: ExternalTool
    ) -> None:
        (self.__dict__.get('out') or Output()).print(
            _("Running baseline scripts with external tool '{tool_path}'").format(
                tool_path=tool.exec_path
            )
        )               
        script_infos = [ScriptFsInfo.get_info(scripts_dir, s) for s in scripts]
        for i in script_infos:
            (self.__dict__.get('out') or Output()).print(_("Running script: {script_info}...").format(script_info=repr(i)))
            tool.run(i.script_path)            
        (self.__dict__.get('out') or Output()).print(f"Setting the baseline version '{version}'...")        
        schema_id = self.get_schema_name()        
        version_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, TRUE);", 
            schema_name=schema_id
        )                                  
        script_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);", 
            schema_name=schema_id
        )
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                cur.execute(version_sql, (version,))                
                for i in script_infos:
                    cur.execute(script_sql, (version, i.relative_path, i.oid))                    
        (self.__dict__.get('out') or Output()).print(_("Committed."))

    def run_baseline_scripts_each_in_own_tran(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path]
    ) -> None:        
        script_infos = [ScriptFsInfo.get_info_with_text(scripts_dir, s) for s in scripts]
        for i in script_infos:
            (self.__dict__.get('out') or Output()).print(_("Running script: {script_info}...").format(script_info=repr(i)))
            with self.dbconn.transaction():
                with self.dbconn.cursor() as cur:
                    cur.execute(i.text)                                  
            (self.__dict__.get('out') or Output()).print(_("Committed."))
        (self.__dict__.get('out') or Output()).print(
            _("Setting the baseline version to: '{version}'.")
            .format(version=version)
        )
        schema_id = self.get_schema_name()
        version_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, TRUE)",  
            schema_name=schema_id)                                  
        script_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);\n", 
            schema_name=schema_id)
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                cur.execute(version_sql, (version,))
                for i in script_infos:
                    cur.execute(script_sql, (version, i.relative_path, i.oid))
        (self.__dict__.get('out') or Output()).print(_("Committed."))

    def rerun_versioned_scripts(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path]
    ) -> None: 
        (self.__dict__.get('out') or Output()).print(_("Reapply version {version}...").format(version=version))
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, self.file_read_encoding, encoding_errors=self.file_read_encoding_errors) for s in scripts]
        schema_id=self.get_schema_name()        
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                formatted_sql = self.format_sql(
                    "DELETE FROM {schema_name}.dbmigration_version_scripts WHERE version_id=%s", schema_name=schema_id)
                cur.execute(formatted_sql, (version,))    
                formatted_sql = self.format_sql(
                    "DELETE FROM {schema_name}.dbmigration_versions WHERE version_id=%s", schema_name=schema_id)
                cur.execute(formatted_sql, (version,))    
                for i in script_infos:
                    (self.__dict__.get('out') or Output()).print(_("Running script: {script_info}...").format(script_info=repr(i)))
                    cur.execute(i.text)                              
                formatted_sql = self.format_sql(
                    "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, FALSE)", schema_name=schema_id)                                  
                cur.execute(formatted_sql, (version,))
                for i in script_infos:
                    formatted_sql = self.format_sql(
                        "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);\n", schema_name=schema_id)
                    cur.execute(formatted_sql, (version, i.relative_path, i.oid))        
        (self.__dict__.get('out') or Output()).print(_("Committed."))

    def run_versioned_scripts_in_tran(
        self, 
        version: str, 
        scripts_dir: Path, 
        scripts: list[Path]
    ) -> None:             
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, self.file_read_encoding, encoding_errors=self.file_read_encoding_errors) for s in scripts]        
        (self.__dict__.get('out') or Output()).print(_("Apply version {version}...").format(version=version))
        schema_id=self.get_schema_name() 
        with self.dbconn.transaction():
            with self.dbconn.cursor() as cur:
                for i in script_infos:
                    (self.__dict__.get('out') or Output()).print(_("Running script: {script_info}...").format(script_info=repr(i)))
                    cur.execute(i.text)
                formatted_sql = self.format_sql(
                    "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES (%s, FALSE)", schema_name=schema_id)                                  
                cur.execute(formatted_sql, (version,))
                for i in script_infos:
                    formatted_sql = self.format_sql(
                        "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES (%s, %s, %s);\n", schema_name=schema_id)
                    cur.execute(formatted_sql, (version, i.relative_path, i.oid))
        (self.__dict__.get('out') or Output()).print(_("Committed."))

    def __init__(
        self,
        opts: UpdateOptions,
        deps: Deps,
        out: Output | None = None,
        confirm: Confirmation | None = None,
    ) -> None:
        super().__init__(opts, deps, out, confirm)

    def apply_baseline_scripts(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            (self.__dict__.get('out') or Output()).print(
                _(
                    "The scripts directory '{scripts_dir}' is missing '{baseline_dir_name}' subdirectory. "
                    "Baseline scripts will be skipped."
                ).format(scripts_dir=scripts_dir, baseline_dir_name=BASELINE_DIR_NAME)
            )
            return
        if self.check_if_version_table_include_baseline_version():
            (self.__dict__.get('out') or Output()).print(_("The target schema already has the baseline version installed. Baseline scripts will be skipped."))
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        baseline_subdirs_len = len(baseline_subdirs)
        if baseline_subdirs_len != 1:
            raise CommandError(
                _(
                    "The baseline path {baseline_dir} must have single subdirectory "
                    "with the baseline scripts but {baseline_subdirs_len} was found"
                ).format(baseline_dir=baseline_dir, baseline_subdirs_len=baseline_subdirs_len)
            )
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name
        (self.__dict__.get('out') or Output()).print(_("The baseline version to install {baseline_version}.").format(baseline_version=baseline_version))      
        (self.__dict__.get('out') or Output()).print(_("Apply baseline scripts..."))
        scripts_sorted = self.get_sorted_scripts_from_dir(
            baseline_version_subdir, BASELINE_FILES_DEPTH, force_run_cleanup = self.opts.force_run_cleanup)
        
        external_tool = ExternalTool.try_get(
            baseline_version_subdir, self.get_schema_name_arg(), self.dbconn_settings, self.config)
        if external_tool:
            self.run_baseline_scripts_with_external_tool(
                baseline_version, scripts_dir, scripts_sorted, external_tool)
        else:
            self.run_baseline_scripts_each_in_own_tran(
                baseline_version, scripts_dir, scripts_sorted)

        (self.__dict__.get('out') or Output()).print(_("Baseline scripts applied."))      

    def reapply_the_latest_version(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            (self.__dict__.get('out') or Output()).print(
                _("The scripts directory '{scripts_dir}' is missing the "
                "required '{versioned_dir_name}' subdirectory.").format(
                    scripts_dir=scripts_dir,
                    versioned_dir_name=VERSIONED_DIR_NAME
                )
            )
            return
        
        latest_installed = self.check_if_any_latest_version_installed()
        (self.__dict__.get('out') or Output()).print(
            _("The latest installed version is {latest_installed}.")
            .format(latest_installed=latest_installed)
        )

        latest_version_dir = versioned_dir.joinpath(latest_installed)
        if not latest_version_dir.is_dir():
            raise CommandError(
                _("There is no subdirectory with scripts that matched to the "
                "latest installed version '{latest_installed}'")
                .format(latest_installed=latest_installed)
            )
        
        scripts_sorted = self.get_sorted_scripts_from_dir(
            latest_version_dir, VERSIONED_FILES_DEPTH, force_run_cleanup=True)
        if not scripts_sorted:
            filters_str = ",".join(self.file_glob_filters)
            raise CommandError(
                _("The scripts subdirectory '{latest_version_dir}' does not "
                "contain any '{filters_str}' scripts")
                .format(
                    latest_version_dir=latest_version_dir,
                    filters_str=filters_str
                )
            )
        
        self.rerun_versioned_scripts(latest_installed, scripts_dir, scripts_sorted)

    def apply_versioned_scripts(self) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        force_run_cleanup = self.opts.force_run_cleanup
        versioned_dir = scripts_dir.joinpath(VERSIONED_DIR_NAME)
        if not versioned_dir.exists():
            (self.__dict__.get('out') or Output()).print(
                _("The scripts directory '{scripts_dir}' is missing "
                "'{versioned_dir_name}' subdirectory. Version scripts "
                "will be skipped.")
                .format(
                    scripts_dir=scripts_dir,
                    versioned_dir_name=VERSIONED_DIR_NAME
                )
            )            
            return

        if not self.check_if_version_table_include_baseline_version():
            raise CommandError(
                _("The baseline version must be installed before running versioned scripts")
            )

        versioned_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir()]
        if not versioned_subdirs:
            raise CommandError(
                _("The versioned scripts path {versioned_dir} must have at "
                "least one subdirectory but nothing was found")
                .format(versioned_dir=versioned_dir)
            )

        latest_installed = self.check_if_any_latest_version_installed()
        (self.__dict__.get('out') or Output()).print(
            _("The latest installed version is {latest_installed}.")
            .format(latest_installed=latest_installed)
        )       

        newer_version_subdirs = [item for item in versioned_subdirs if item.name > latest_installed]
        if not newer_version_subdirs:
            (self.__dict__.get('out') or Output()).print(_("No newer versions found for installation."))       
            return

        (self.__dict__.get('out') or Output()).print(
            _("Found {new_versions_count} new versions for installation.")
            .format(new_versions_count=len(newer_version_subdirs))
        )   
        (self.__dict__.get('out') or Output()).print(_("Apply versioned scripts..."))

        sorted_subdirs = sorted(newer_version_subdirs)

        for version_dir in sorted_subdirs:        
            version_id = version_dir.name
            scripts_sorted = self.get_sorted_scripts_from_dir(
                version_dir, VERSIONED_FILES_DEPTH, force_run_cleanup=force_run_cleanup
            )
            
            if not scripts_sorted:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(
                    _("The scripts subdirectory '{version_dir}' does not "
                    "contain any '{filters_str}' scripts")
                    .format(
                        version_dir=version_dir,
                        filters_str=filters_str
                    )
                )
                
            self.run_versioned_scripts_in_tran(version_id, scripts_dir, scripts_sorted)       

        (self.__dict__.get('out') or Output()).print(_("Versioned scripts applied."))

    def apply_repeatable_scripts(self, force_reapply: bool = False) -> None:        
        scripts_dir = self.get_resolved_scripts_dir()
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            (self.__dict__.get('out') or Output()).print(
                _("The scripts directory '{scripts_dir}' is missing the "
                "required '{repeatable_dir_name}' subdirectory.")
                .format(
                    scripts_dir=scripts_dir,
                    repeatable_dir_name=REPEATABLE_DIR_NAME
                )
            )
            return

        (self.__dict__.get('out') or Output()).print(_("Check repeatable scripts..."))
        target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(
                _("The file with target version '{target_version_file}' does not "
                "exist in repeatable scripts subdirectory '{repeatable_dir}'.")
                .format(
                    target_version_file=TARGET_VERSION_FILE,
                    repeatable_dir=repeatable_dir
                )
            )
        target_version = read_as_trimmed_string(target_version_file_path)

        latest_installed_version = self.check_if_any_latest_version_installed() 
        if latest_installed_version != target_version:
            raise CommandError(
                _("The target version {target_version} for repeatable scripts "
                "does not match the latest installed version "
                "'{latest_installed_version}'.")
                .format(
                    target_version=target_version,
                    latest_installed_version=latest_installed_version
                )
            )

        (self.__dict__.get('out') or Output()).print(
            _("Target version matches the latest installed version: "
            "'{target_version}'.")
            .format(target_version=target_version)
        )

        repeatable_scripts_sorted = self.get_sorted_scripts_from_dir(repeatable_dir, REPEATABLE_FILES_DEPTH)

        scripts_to_repeat = []
        if force_reapply:
            scripts_to_repeat = [*repeatable_scripts_sorted]
        else:
            script_infos = [
                ScriptFsInfo.get_info(scripts_dir, s) for s in repeatable_scripts_sorted
            ]
            scripts_to_repeat = [
                i.script_path
                for i in script_infos
                if not self.check_if_repeatable_script_installed(i.oid, target_version, i.relative_path)
            ]

        if not scripts_to_repeat:
            (self.__dict__.get('out') or Output()).print(_("No modified repeatable scripts found for (re)installation."))       
            return

        scripts_to_repeat = self.resolve_scripts_dependencies(
            repeatable_dir, REPEATABLE_FILES_DEPTH, repeatable_scripts_sorted, scripts_to_repeat
        )
        
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=self.file_read_encoding, encoding_errors=self.file_read_encoding_errors
            ) 
            for s in scripts_to_repeat
        ]
        
        (self.__dict__.get('out') or Output()).print(
            _("Found {scripts_count} scripts to re-run")
            .format(scripts_count=len(script_infos))
        )
        (self.__dict__.get('out') or Output()).print(_("Apply repeatable scripts..."))

        schema_id = self.get_schema_name()
        repeatable_sql = self.format_sql(
            "INSERT INTO {schema_name}.dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES (%s, %s, %s)", 
            schema_name=schema_id
        )                                  

        for i in script_infos:
            (self.__dict__.get('out') or Output()).print(_("Running script: {script_info}...").format(script_info=repr(i)))
            with self.dbconn.transaction():
                with self.dbconn.cursor() as cur:
                    cur.execute(i.text)
                    cur.execute(repeatable_sql, (i.oid, target_version, i.relative_path))
            (self.__dict__.get('out') or Output()).print(_("Committed."))

        (self.__dict__.get('out') or Output()).print(_("Repeatable scripts applied."))

    def _run(self) -> int:
        if not self.opts.skip_confirmation:
            if not self.confirm(_("You are going to run updates. Would you like to continue? [y/N]: ")):
                raise CommandError(_("Cancelled by user"))
        
        self.do_initial_cross_checks()        
        
        applied_count = self.apply_all_own_migrations()
        if applied_count > 0:
            (self.__dict__.get('out') or Output()).print(_("Version control tables updated. Please rerun the tool to update the schema using your scripts."))
            return 0

        self.check_if_all_version_control_tables_exist()
        self.check_if_stored_environment_id_matches_to_scripts_dir() 

        scripts_dir = self.get_scripts_path_arg()        
        if self.opts.force_reapply_latest_version:
            (self.__dict__.get('out') or Output()).print(
                _("Performing reapply latest version from scripts "
                "repository: '{scripts_dir}'")
                .format(scripts_dir=scripts_dir)
            )
            self.reapply_the_latest_version()
            self.apply_repeatable_scripts(force_reapply=True)
            (self.__dict__.get('out') or Output()).print(_("Reapplied."))
        else:
            (self.__dict__.get('out') or Output()).print(
                _("Performing updates from scripts repository: '{scripts_dir}'")
                .format(scripts_dir=scripts_dir)
            )
            self.check_if_max_version_of_versioned_scripts_matches_repeatable_target()
            self.apply_baseline_scripts()
            self.apply_versioned_scripts()
            self.apply_repeatable_scripts(force_reapply=self.opts.force_reapply_all_repeatable)
            (self.__dict__.get('out') or Output()).print(_("Updated."))
        return 0
