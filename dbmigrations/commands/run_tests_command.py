"""RunTestsCommand and the TestFailed exception."""

from pathlib import Path
from typing import Any, Iterable

import psycopg

from _constants import (
    ASSURE_THAT_TEST_PREFIX,
    DETECT_MISSING_TEST_PREFIX,
    IS_TRUE_THAT_TEST_PREFIX,
    SETUP_TESTS_FILE_NAME,
    TARGET_VERSION_FILE,
    TESTS_DIR_NAME,
    TESTS_FILES_DEPTH,
)
from _confirmation import Confirmation
from _errors import CommandError
from _i18n import _
from _options import Deps, RunTestsOptions
from _output import Output
from _scripts import ScriptFsInfo, read_as_trimmed_string

from commands.base import BaseCommand


class TestFailed(Exception):
    """A unit test error."""
class RunTestsCommand (BaseCommand):
    """Runs db unit test scripts to the target database schema."""

    def run_conditional(self, cursor: Any, script: ScriptFsInfo) -> None:
        (self.__dict__.get('out') or Output()).print(
            _("Running test: '{relative_script_path}'...")
            .format(relative_script_path=script.relative_path),
            end="",
            flush=True
        )
        
        file_name = script.script_path.name

        if file_name.startswith(IS_TRUE_THAT_TEST_PREFIX):
            self._run_is_true_test(cursor, script.text)
        elif file_name.startswith(DETECT_MISSING_TEST_PREFIX):
            self._run_detect_missing_test(cursor, script.text)
        elif file_name.startswith(ASSURE_THAT_TEST_PREFIX):
            cursor.execute(script.text)
        else:
            raise TestFailed(
                _(
                    "Unable to detect test type from script name '{file_name}'. It should "
                    "start with one of the following prefixes: '{is_true_prefix}',"
                    "'{detect_missing_prefix}','{assure_that_prefix}'"
                ).format(
                    file_name=file_name,
                    is_true_prefix=IS_TRUE_THAT_TEST_PREFIX,
                    detect_missing_prefix=DETECT_MISSING_TEST_PREFIX,
                    assure_that_prefix=ASSURE_THAT_TEST_PREFIX,
                )
            )
            
        (self.__dict__.get('out') or Output()).print(_("PASS"))

    def _run_is_true_test(self, cursor: Any, script_text: str) -> None:
        cursor.execute(script_text)
        for res_num, results in enumerate(cursor.results(), start=1):
            if cursor.rowcount <= 0:
                continue
                
            row = cursor.fetchone()
            value = row[0] if row is not None else False
            if not value:
                raise TestFailed(
                    _("({result_number}) Expected true, got {value}!")
                    .format(result_number=res_num, value=value)
                )

    def _run_detect_missing_test(self, cursor: Any, script_text: str) -> None:
        cursor.execute(script_text)
        has_failed = False
        
        for res_num, results in enumerate(cursor.results(), start=1):
            if cursor.rowcount <= 0:
                continue
                
            columns = [desc[0] for desc in cursor.description]
            (self.__dict__.get('out') or Output()).print(
                _("FAIL. ({result_number}) Missing records:")
                .format(result_number=res_num)
            )
            (self.__dict__.get('out') or Output()).print("=================================")
            
            for row in results:
                items = [f"{k}: {v}" for k, v in zip(columns, row)]
                (self.__dict__.get('out') or Output()).print(", ".join(items))
                
            has_failed = True
            
        if has_failed:
            raise TestFailed(_("Expected no results!"))

    def is_subpath_of(self, child: Path, parent: Path) -> bool:
        child_parts = Path(child).absolute().parts
        parent_parts = Path(parent).absolute().parts        
        return child_parts[:len(parent_parts)] == parent_parts
    
    def make_savepoint_id(self, folder: Path) -> psycopg.sql.Identifier:
        hash_str = str(hash(folder))
        return psycopg.sql.Identifier("savepoint_" + hash_str)

    def run_test_scripts_each_in_own_tran(self, scripts: Iterable[ScriptFsInfo]) -> None:
        self.fail_count = 0
        self.pass_count = 0
        
        with self.dbconn.cursor() as cur:
            cur.execute("BEGIN")
            try:
                setup_folder_stack: list[str] = []            
                
                for script in scripts:
                    self._rollback_outdated_setups(cur, script, setup_folder_stack)
                    
                    if script.script_path.name == SETUP_TESTS_FILE_NAME:
                        self._run_setup_script(cur, script, setup_folder_stack)
                        continue
                        
                    self._run_single_test_with_boundary(cur, script)
            finally:
                cur.execute("ROLLBACK")


    def _rollback_outdated_setups(self, cur: Any, script: ScriptFsInfo, stack: list[str]) -> None:
        if not stack:
            return
            
        script_folder = str(script.script_path.absolute().parent)
        latest_item = stack[-1]
        
        if not self.is_subpath_of(script_folder, latest_item):
            stack.pop()
            savepoint_id = self.make_savepoint_id(latest_item)
            
            formatted_sql = self.format_sql("ROLLBACK TO SAVEPOINT {savepoint_id}", savepoint_id=savepoint_id)
            cur.execute(formatted_sql)
            (self.__dict__.get('out') or Output()).print(_("Rolled back to savepoint."))


    def _run_setup_script(self, cur: Any, script: ScriptFsInfo, stack: list[str]) -> None:
        setup_folder = str(script.script_path.absolute().parent)
        stack.append(setup_folder)
        
        savepoint_id = self.make_savepoint_id(setup_folder)
        (self.__dict__.get('out') or Output()).print(_("Make savepoint..."))
        
        formatted_sql = self.format_sql("SAVEPOINT {savepoint_id}", savepoint_id=savepoint_id)
        cur.execute(formatted_sql)
        
        (self.__dict__.get('out') or Output()).print(
            _("Running setup: '{relative_script_path}'...")
            .format(relative_script_path=script.relative_path),
            end="",
            flush=True
        )
        cur.execute(script.text)
        (self.__dict__.get('out') or Output()).print(_("DONE"))


    def _run_single_test_with_boundary(self, cur: Any, script: ScriptFsInfo) -> None:
        cur.execute("SAVEPOINT savepoint_test_boundary")
        try:
            self.run_conditional(cur, script)
            self.pass_count += 1
        except TestFailed as e:
            self.fail_count += 1
            (self.__dict__.get('out') or Output()).print(_("FAIL."), e)
        except Exception as e:
            self.fail_count += 1
            error_name = type(e).__name__ 
            (self.__dict__.get('out') or Output()).print(
                _("FAIL. {error_type_name}:").format(error_type_name=error_name),
                e
            )
        finally:
            cur.execute("ROLLBACK TO SAVEPOINT savepoint_test_boundary")

    def __init__(
        self,
        opts: RunTestsOptions,
        deps: Deps,
        out: Output | None = None,
        confirm: Confirmation | None = None,
    ) -> None:
        super().__init__(opts, deps, out, confirm)

    def run_unit_test_scripts(self, scripts_dir: Path) -> None:
        unit_tests_dir = scripts_dir.joinpath(TESTS_DIR_NAME)
        if not unit_tests_dir.exists():
            raise CommandError(
                _("The scripts directory '{scripts_dir}' is missing the required "
                "'{tests_dir_name}' subdirectory.")
                .format(scripts_dir=scripts_dir, tests_dir_name=TESTS_DIR_NAME)
            )

        if not self.opts.skip_env_checks:
            target_version_file_path = unit_tests_dir.joinpath(TARGET_VERSION_FILE)
            if not target_version_file_path.exists():
                raise CommandError(
                    _("The file with target version '{target_version_file}' does not "
                    "exists in unit tests scripts subdirectory '{unit_tests_dir}'.")
                    .format(
                        target_version_file=TARGET_VERSION_FILE,
                        unit_tests_dir=unit_tests_dir
                    )
                )
            target_version = read_as_trimmed_string(target_version_file_path)
            latest_installed_version = self.check_if_any_latest_version_installed() 
            if latest_installed_version != target_version:
                raise CommandError(
                    _("The target version {target_version} for unit test scripts "
                    "does not match the latest installed version "
                    "{latest_installed_version}.")
                    .format(
                        target_version=target_version,
                        latest_installed_version=latest_installed_version
                    )
                )                  
            (self.__dict__.get('out') or Output()).print(
                _("Target version matches the latest installed version: "
                "'{target_version}'")
                .format(target_version=target_version)
            )

        scripts_sorted = self.get_sorted_scripts_from_dir(unit_tests_dir, TESTS_FILES_DEPTH)        
        script_infos = [
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, 
                encoding=self.file_read_encoding, encoding_errors=self.file_read_encoding_errors)
            for s in scripts_sorted
        ]
        self.run_test_scripts_each_in_own_tran(script_infos)
        if self.fail_count > 0:
            raise CommandError(
                _("Tests failed: {fail_count}, passed: {pass_count}.")
                .format(
                    fail_count=self.fail_count,
                    pass_count=self.pass_count
                )
            )
        else:
            (self.__dict__.get('out') or Output()).print(
                _("All {pass_count} tests passed.")
                .format(pass_count=self.pass_count)
            )            

    def _run(self) -> int:
        self.do_initial_cross_checks()
        if not self.opts.skip_env_checks:
            self.check_if_all_own_migrations_are_applied()
            self.check_if_all_version_control_tables_exist() 
            self.check_if_stored_environment_id_matches_to_scripts_dir()
        scripts_dir = self.get_resolved_scripts_dir()    
        (self.__dict__.get('out') or Output()).print(
            _("Running unit tests on scripts repository: '{scripts_dir}'")
            .format(scripts_dir=scripts_dir)
        )
        self.run_unit_test_scripts(scripts_dir)
        return 0
