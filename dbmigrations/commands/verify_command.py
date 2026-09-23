"""VerifyCommand and UpdateScriptBuilder."""

import collections
from pathlib import Path
from types import TracebackType
from typing import Any, Iterable, Self, TextIO, Type

from psycopg.rows import TupleRow

from _constants import (
    BASELINE_DIR_NAME,
    BASELINE_FILES_DEPTH,
    RECENT_CHANGES_LIMIT,
    RECENT_CHANGES_WINDOW_MINUTES,
    REPEATABLE_DIR_NAME,
    REPEATABLE_FILES_DEPTH,
    TARGET_VERSION_FILE,
    VERSIONED_DIR_NAME,
    VERSIONED_FILES_DEPTH,
)
from _confirmation import Confirmation
from _errors import CommandError
from _git import GitChecker
from _i18n import _
from _options import Deps, VerifyOptions
from _output import Output
from _scripts import (
    ScriptDbInfo,
    ScriptFsInfo,
    read_as_trimmed_string,
    render_script_diff_text,
)

from commands.base import BaseCommand


class UpdateScriptBuilder:
    target_script_path: Path
    temp_script_path: Path
    written_body_bytes: int
    temp_file: TextIO | None

    def __init__(self, script_path: Path | str, out: Output | None = None) -> None:
        self.out = out if out is not None else Output()
        self.target_script_path = Path(script_path)
        self.temp_script_path = Path(script_path).with_suffix(".temp")
        self.written_body_bytes = 0
        self.temp_file = None
    
    def check(self) -> None:
        assert self.target_script_path is not None, _("self.target_script_path must be initialized")
        assert isinstance(self.target_script_path, Path), _("self.target_script_path must be a pathlib.Path")

        if not self.target_script_path.parent.exists():
            raise CommandError(
                _("The parent directory '{parent_dir}' does not exist")
                .format(parent_dir=self.target_script_path.parent)
            )
        try:
            self.target_script_path.touch(exist_ok=False)
        except FileExistsError:
            raise CommandError(
                _("The specified script file '{target_script_path}' already exists")
                .format(target_script_path=self.target_script_path)
            )
        except PermissionError:
            raise CommandError(
                _("The specified script file '{target_script_path}' is not "
                "accessible for write")
                .format(target_script_path=self.target_script_path)
            )
        except OSError as e:
            raise CommandError(
                _("System error while verifying path '{target_script_path}': {error}")
                .format(
                    target_script_path=self.target_script_path,
                    error=e
                )
            )    
        try:
            self.temp_script_path.open("w").close()
        except Exception as e:
            raise CommandError(
                _("Unable to write to temporary target script file '{temp_script_path}'")
                .format(temp_script_path=self.temp_script_path)
            )

    def get_written_body_bytes(self) -> int:
        return self.written_body_bytes

    def __enter__(self) -> Self:
        assert self.temp_script_path is not None, _("self.temp_script_path must be initialized")
        assert isinstance(self.temp_script_path, Path), _("self.temp_script_path must be a pathlib.Path")
        self.temp_file = self.temp_script_path.open("a", encoding="utf-8")
        return self

    def __exit__(
        self, 
        exc_type: Type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: TracebackType | None
    ) -> bool:
        if self.temp_file and not self.temp_file.closed:
            self.temp_file.close()        
        if exc_type is not None:
            try:
                self.temp_script_path.unlink()
            except Exception:
                pass 
            try:
                self.target_script_path.unlink()
            except Exception:
                pass 
        return False 

    def write_header(self, s: str) -> None:
        assert self.temp_file is not None, "The temporary file is not initialized yet. Ensure you are inside the 'with' context."
        assert not self.temp_file.closed, "The temporary file is not opened. Ensure you are inside the 'with' context."
        self.temp_file.write(s)

    def write_body(self, s: str) -> None:
        assert self.temp_file is not None, "The temporary file is not initialized yet. Ensure you are inside the 'with' context."
        assert not self.temp_file.closed, "The temporary file is not opened. Ensure you are inside the 'with' context."

        written = self.temp_file.write(s)
        if written > 0:
            self.written_body_bytes += written

    def write_body_lines(self, lines: Iterable[str]) -> None:
        assert self.temp_file is not None, "The temporary file is not initialized yet. Ensure you are inside the 'with' context."
        assert not self.temp_file.closed, "The temporary file is not opened. Ensure you are inside the 'with' context."
        for s in lines:
            written = self.temp_file.write(s)
            if written > 0:
                self.written_body_bytes += written
    
    def cleanup(self) -> None:
        if self.temp_file and not self.temp_file.closed:
            self.temp_file.close()

        if self.temp_script_path is not None:
            try:
                self.temp_script_path.unlink()
            except Exception as e:
                message = str(e)
                (self.__dict__.get('out') or Output()).print(
                    _("Warning: Unable cleanup temporary file '{temp_script_path}'. "
                    "Inner error: {error}")
                    .format(
                        temp_script_path=self.temp_script_path,
                        error=message
                    )
                )
        else:
            (self.__dict__.get('out') or Output()).print(
                _("Warning: The temporary script path is not initialized")
            )

        if self.target_script_path is not None:
            try:
                self.target_script_path.unlink()
            except Exception as e:
                message = str(e)
                (self.__dict__.get('out') or Output()).print(
                    _("Warning: Unable cleanup target file '{target_script_path}'. "
                    "Inner error: {error}")
                    .format(
                        target_script_path=self.target_script_path,
                        error=message
                    )
                )
        else:
            (self.__dict__.get('out') or Output()).print(
                _("Warning: The target script path is not initialized")
            )

    def finalize(self) -> None:
        assert isinstance(self.temp_script_path, Path)
        assert isinstance(self.target_script_path, Path)

        if self.temp_file and not self.temp_file.closed:
            self.temp_file.close()

        try:
            if self.target_script_path.exists():
                self.target_script_path.unlink()
            self.temp_script_path.rename(self.target_script_path)
        except Exception as e:
            message = str(e)
            raise CommandError(
                _("Unable to rename temporary file '{temp_script_path}' to the "
                "target script file {target_script_path}. Inner error: {error}")
                .format(
                    temp_script_path=self.temp_script_path,
                    target_script_path=self.target_script_path,
                    error=message
                )
            )
class VerifyCommand (BaseCommand):
    """Validates the target schema and lists versioned and reproducible scripts to apply if the 'update' command is executed."""
    
    def make_dbconn_session_readonly(self) -> None:
        sql = """
            SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"""
        with self.dbconn.cursor() as cur:
            cur.execute(sql)

    def get_baseline_version_installed(self) -> str|None:
        sql = """
                SELECT version_id FROM {schema_name}.dbmigration_versions WHERE is_baseline IS TRUE ORDER BY version_id DESC LIMIT 1"""
        schema_id = self.get_schema_name()
        formatted_sql = self.format_sql(sql, schema_name=schema_id)
        value = self.dbconn_get_single_value(formatted_sql, [])
        return value

    def cross_check_of_the_target_version_for_repeatable_scripts(self, target_version, latest_version_in_scripts, latest_installed_version):
        if latest_version_in_scripts is None and latest_installed_version is None:
            raise CommandError(
                _("Failed to check target version '{target_version}' because no "
                "version is installed and no versioned scripts were provided "
                "in the scripts directory.")
                .format(target_version=target_version)
            )
        elif latest_version_in_scripts is None:
            if target_version != latest_installed_version:
                raise CommandError(
                    _("The target version '{target_version}' does not match the latest "
                    "installed version '{latest_installed_version}'.")
                    .format(
                        target_version=target_version,
                        latest_installed_version=latest_installed_version
                    )
                )
        elif latest_installed_version is None:
            if target_version != latest_version_in_scripts:
                raise CommandError(
                    _("The target version '{target_version}' does not match the latest "
                    "scripts version '{latest_version_in_scripts}'.")
                    .format(
                        target_version=target_version,
                        latest_version_in_scripts=latest_version_in_scripts
                    )
                )
        elif latest_version_in_scripts > latest_installed_version:
            if target_version != latest_version_in_scripts:
                raise CommandError(
                    _("The target version '{target_version}' does not match the latest "
                    "scripts version '{latest_version_in_scripts}'.")
                    .format(
                        target_version=target_version,
                        latest_version_in_scripts=latest_version_in_scripts
                    )
                )
        elif latest_version_in_scripts <= latest_installed_version:
            if target_version != latest_installed_version:
                raise CommandError(
                    _("The target version '{target_version}' does not match the latest "
                    "installed version '{latest_installed_version}'.")
                    .format(
                        target_version=target_version,
                        latest_installed_version=latest_installed_version
                    )
                )
    
    def get_stored_oid_for_script(
        self,
        i: ScriptFsInfo,
        version: str | None = None,
        scripts_table: str = "versioned",
    ) -> str | None:
        """Returns the git blob OID applied to the database for a script, if any."""
        if scripts_table == "repeatable" and version is not None:
            return self.get_db_oid_for_repeatable_script(i.relative_path, version)
        return self.get_db_oid_for_versioned_script(i.relative_path, version)

    def build_script_diff_text(
        self,
        i: ScriptFsInfo,
        version: str | None = None,
        scripts_table: str = "versioned",
    ) -> str | None:
        """
        Builds the unified text diff between the script version applied in the database
        (identified by the stored git blob OID) and the current script file in the repository.
        Returns an empty string when the texts are identical, a short marker when the
        script is new (not applied yet), None when the applied content is missing
        (a notice was already printed), or the unified diff text otherwise.
        """
        assert self.git is not None

        stored_oid = self.get_stored_oid_for_script(i, version, scripts_table)
        if stored_oid is not None:
            old_text = self.git.get_blob_content_by_oid(stored_oid)
            if old_text is None:
                (self.__dict__.get('out') or Output()).print(
                    _(
                        "Unable to display diff for '{relative_path}' because the applied "
                        "content (git OID: {stored_oid}) was not found in the local repository."
                    ).format(relative_path=i.relative_path, stored_oid=stored_oid)
                )
                return None
        else:
            old_text = ""
            stored_oid = None

        if old_text == i.text:
            return ""

        if stored_oid is None:
            return _("+ New file, will be applied in full.")

        return render_script_diff_text(old_text, i.text, i.relative_path, stored_oid, i.oid)

    def _print_diff(self, diff_text: str | None, indent: str) -> None:
        """Prints indented diff lines (an empty diff or a missing blob is printed as nothing)."""
        if diff_text is None or diff_text == "":
            return
        for line in diff_text.splitlines():
            (self.__dict__.get('out') or Output()).print(f"{indent}{line}")

    def display_script_entry(
        self,
        i: ScriptFsInfo,
        version: str | None = None,
        scripts_table: str = "versioned",
        indent: str = "    ",
    ) -> None:
        """Prints a script entry followed inline by its text diff (unless skipped)."""
        (self.__dict__.get('out') or Output()).print(f"{indent}{i!r}")
        if self.opts.skip_diffs or self.git is None:
            return
        diff_text = self.build_script_diff_text(i, version, scripts_table)
        self._print_diff(diff_text, indent)

    def display_required_changes_by_commits(
        self,
        script_infos: list[ScriptFsInfo],
        version: str | None = None,
        scripts_table: str = "versioned",
    ) -> None:
        assert self.git is not None

        commits_group = collections.defaultdict(list)        
        for i in script_infos:
            commit_info = self.git.get_latest_commit(i.script_path)
            commits_group[commit_info].append(i)

        sorted_commits = sorted(
            commits_group.items(),
            key=lambda i: i[0].sort_key(), 
            reverse=True
        )

        for commit, scripts in sorted_commits:
            (self.__dict__.get('out') or Output()).print(f"{commit!r}")
            for s in scripts:
                self.display_script_entry(s, version=version, scripts_table=scripts_table)

    def get_db_oid_for_versioned_script(
        self, relative_path: str, version_id: str | None = None
    ) -> str | None:
        """Returns the git blob OID applied to the database for a versioned script, if any."""
        if version_id is not None:
            sql = """
                SELECT git_blob_sha1
                FROM {schema_name}.dbmigration_version_scripts
                WHERE relative_path = %s AND version_id = %s
                ORDER BY version_id DESC
                LIMIT 1
            """
            params = (relative_path, version_id)
        else:
            sql = """
                SELECT git_blob_sha1
                FROM {schema_name}.dbmigration_version_scripts
                WHERE relative_path = %s
                ORDER BY version_id DESC
                LIMIT 1
            """
            params = (relative_path,)
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        return self.dbconn_get_single_value(formatted_sql, params)

    def get_db_oid_for_repeatable_script(self, relative_path: str, version_id: str) -> str | None:
        """Returns the latest git blob OID applied to the database for a repeatable script, if any."""
        sql = """
            SELECT git_blob_sha1
            FROM {schema_name}.dbmigration_repeatable_scripts
            WHERE relative_path = %s AND version_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        return self.dbconn_get_single_value(formatted_sql, (relative_path, version_id))

    def get_previous_db_oid_for_repeatable_script(
        self, relative_path: str, version_id: str, before_applied_at: Any
    ) -> str | None:
        """Returns the git blob OID previously applied for a repeatable script before the given time, if any."""
        sql = """
            SELECT git_blob_sha1
            FROM {schema_name}.dbmigration_repeatable_scripts
            WHERE relative_path = %s AND version_id = %s AND created_at < %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        return self.dbconn_get_single_value(formatted_sql, (relative_path, version_id, before_applied_at))

    def get_previous_db_oid_for_versioned_script(
        self, relative_path: str, version_id: str
    ) -> str | None:
        """Returns the git blob OID previously applied for a versioned script in earlier versions, if any."""
        sql = """
            SELECT git_blob_sha1
            FROM {schema_name}.dbmigration_version_scripts
            WHERE relative_path = %s AND version_id < %s
            ORDER BY version_id DESC
            LIMIT 1
        """
        formatted_sql = self.format_sql(sql, schema_name=self.get_schema_name())
        return self.dbconn_get_single_value(formatted_sql, (relative_path, version_id))

    def display_required_changes(
        self,
        script_infos: list[ScriptFsInfo],
        version: str | None = None,
        scripts_table: str = "versioned",
    ) -> None:
        if self.git is None:
            for i in script_infos:
                self.display_script_entry(i, version=version, scripts_table=scripts_table, indent="  ")
        else:
            self.display_required_changes_by_commits(script_infos, version=version, scripts_table=scripts_table)

    def display_required_changes_by_path(
        self,
        scripts_dir: Path,
        scripts_sorted: list[Path],
        version: str | None = None,
        scripts_table: str = "versioned",
    ) -> None:
        if self.git is not None and not self.opts.skip_diffs:
            script_infos = [
                ScriptFsInfo.get_info_with_text(
                    scripts_dir, s, encoding=self.file_read_encoding, encoding_errors=self.file_read_encoding_errors
                )
                for s in scripts_sorted
            ]
        else:
            script_infos = [ScriptFsInfo.get_info(scripts_dir, s) for s in scripts_sorted]
        self.display_required_changes(script_infos, version=version, scripts_table=scripts_table)

    def get_recent_changes_from_db(self, limit:int, window_minutes:int) -> list[TupleRow]:
        sql = """
            WITH latest_time AS (
                SELECT COALESCE(MAX(applied_at), NOW()) AS max_at
                FROM (
                    SELECT MAX(v.created_at) AS applied_at FROM {schema_name}.dbmigration_versions v
                    UNION ALL
                    SELECT MAX(r.created_at) AS applied_at FROM {schema_name}.dbmigration_repeatable_scripts r
                ) t
            )
            SELECT 
                v.created_at AS applied_at,
                'versioned' AS script_type,
                s.version_id,
                s.relative_path,
                s.git_blob_sha1
            FROM {schema_name}.dbmigration_version_scripts s
            JOIN {schema_name}.dbmigration_versions v ON s.version_id = v.version_id
            CROSS JOIN latest_time
            WHERE v.created_at >= latest_time.max_at - ({window_minutes} || ' minutes')::interval

            UNION ALL

            SELECT 
                r.created_at AS applied_at,
                'repeatable' AS script_type,
                r.version_id,
                r.relative_path,
                r.git_blob_sha1
            FROM {schema_name}.dbmigration_repeatable_scripts r
            CROSS JOIN latest_time
            WHERE r.created_at >= latest_time.max_at - ({window_minutes} || ' minutes')::interval

            ORDER BY applied_at DESC
            LIMIT {limit};

        """
        schema_id = self.get_schema_name()
        formatted_sql = self.format_sql(sql, schema_name=schema_id, limit=limit, window_minutes=window_minutes)        
        with self.dbconn.cursor() as cursor:
            cursor.execute(formatted_sql, [])
            rows = cursor.fetchall()
        return rows

    def display_recent_changes_grouped_by_git_commits(
        self,
        rows: list[TupleRow],
        diff_map: dict[str, str | None] | None = None,
    ) -> None:
        assert self.git is not None

        commits_group = collections.defaultdict(list)

        for applied_at, script_type, version_id, relative_path, git_blob_sha1 in rows:
            script_info = ScriptDbInfo(
                applied_at=applied_at, 
                script_type=script_type, 
                version_id=version_id, 
                relative_path=relative_path, 
                git_blob_sha1=git_blob_sha1)
            clean_oid = git_blob_sha1.strip()
            commit_info = self.git.get_commit_by_file_oid(clean_oid)
            commits_group[commit_info].append(script_info)
        
        sorted_commits = sorted(
            commits_group.items(), 
            key=lambda i: i[0].sort_key(),
            reverse=True
        )
        seen_paths: set[str] = set()
        for commit, scripts in sorted_commits:
            (self.__dict__.get('out') or Output()).print(f"{commit!r}")
            for s in scripts:
                (self.__dict__.get('out') or Output()).print(f"  {s!r}")
                if diff_map is None or s.relative_path in seen_paths:
                    continue
                seen_paths.add(s.relative_path)
                self._print_diff(diff_map.get(s.relative_path), "    ")

    def build_recent_changes_diff_map(self, rows: list[TupleRow]) -> dict[str, str | None]:
        """
        Builds {relative_path: diff_text} for the most recent application per file.
        diff_text is an empty string for identical texts, None when a notice about a
        missing blob was already printed, a short marker for brand-new scripts, or the
        unified diff text (previously applied vs applied) otherwise.
        """
        assert self.git is not None

        diff_map: dict[str, str | None] = {}
        seen_paths: set[str] = set()
        for applied_at, script_type, version_id, relative_path, git_blob_sha1 in rows:
            if relative_path in seen_paths:
                continue
            seen_paths.add(relative_path)

            cur_oid = git_blob_sha1.strip()
            if script_type == "repeatable":
                prev_oid = self.get_previous_db_oid_for_repeatable_script(
                    relative_path, version_id, applied_at
                )
            else:
                prev_oid = self.get_previous_db_oid_for_versioned_script(relative_path, version_id)

            old_text = ""
            old_oid = None
            if prev_oid is not None:
                old_text = self.git.get_blob_content_by_oid(prev_oid)
                if old_text is None:
                    (self.__dict__.get('out') or Output()).print(
                        _(
                            "Unable to display diff for '{relative_path}' because the applied "
                            "content (git OID: {stored_oid}) was not found in the local repository."
                        ).format(relative_path=relative_path, stored_oid=prev_oid)
                    )
                    diff_map[relative_path] = None
                    continue
                old_oid = prev_oid

            new_text = self.git.get_blob_content_by_oid(cur_oid)
            if new_text is None:
                (self.__dict__.get('out') or Output()).print(
                    _(
                        "Unable to display diff for '{relative_path}' because the applied "
                        "content (git OID: {stored_oid}) was not found in the local repository."
                    ).format(relative_path=relative_path, stored_oid=cur_oid)
                )
                diff_map[relative_path] = None
                continue

            if old_text == new_text:
                diff_map[relative_path] = ""
                continue

            if old_oid is None:
                diff_map[relative_path] = _("+ New file (first application).")
                continue

            diff_map[relative_path] = render_script_diff_text(old_text, new_text, relative_path, old_oid, cur_oid)
        return diff_map

    def display_recent_changes(self, limit:int = 10, window_minutes:int = 30) -> None:
        
        rows = self.get_recent_changes_from_db(limit, window_minutes)
        if not rows:
            return
        (self.__dict__.get('out') or Output()).print(_("The list of recent changes were applied to the target schema:"))

        if self.git is None:
            for applied_at, script_type, version_id, relative_path, git_blob_sha1 in rows:
                script_info = ScriptDbInfo(
                    applied_at=applied_at, 
                    script_type=script_type, 
                    version_id=version_id, 
                    relative_path=relative_path, 
                    git_blob_sha1=git_blob_sha1)
                (self.__dict__.get('out') or Output()).print(f"  {script_info!r}")
        else:
            diff_map = None if self.opts.skip_diffs else self.build_recent_changes_diff_map(rows)
            self.display_recent_changes_grouped_by_git_commits(rows, diff_map=diff_map)

    def __init__(
        self,
        opts: VerifyOptions,
        deps: Deps,
        out: Output | None = None,
        confirm: Confirmation | None = None,
    ) -> None:
        super().__init__(opts, deps, out, confirm)
        self.latest_version_in_scripts: str | None = None

    def write_search_path(self, search_path: str, builder: UpdateScriptBuilder) -> None:
        with builder:
            sql_comment = self.format_sql_comment(f"Setting session search path to: {search_path}")
            builder.write_header(sql_comment)
            sql_text = self.format_sql_text(
                "SELECT pg_catalog.set_config('search_path', {search_path}, false);\n\n", search_path=search_path)
            builder.write_header(sql_text)

    def write_baseline_scripts(self, version: str, scripts_dir: Path, scripts: list[Path], script_builder: UpdateScriptBuilder) -> None:
        encoding = self.file_read_encoding 
        errors = self.file_read_encoding_errors 
        script_infos = [ 
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=encoding, encoding_errors=errors) for s in scripts
        ]
        with script_builder:
            sql_comment = self.format_sql_comment(f"-- --------- BASELINE VERSION: {version} ---------")
            script_builder.write_body(sql_comment)
            for i in script_infos:
                script_builder.write_body(f"BEGIN;\n")
                sql_comment = self.format_sql_comment(f"-- Apply script: [{i.relative_path} (OID:{i.oid:.8})]")                    
                script_builder.write_body(sql_comment)
                script_builder.write_body_lines(i.text)
                script_builder.write_body(f"\n-- End of script.\n")
                script_builder.write_body(f"COMMIT;\n")
            schema_id = self.get_schema_name()
            script_builder.write_body(f"BEGIN;\n")
            sql_text = self.format_sql_text(
                "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, TRUE);\n", 
                schema_name=schema_id, version_id=version)
            script_builder.write_body(sql_text)
            for i in script_infos:
                sql_text = self.format_sql_text(
                    "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                    schema_name=schema_id, version_id=version,relative_path=i.relative_path,git_blob_sha1=i.oid)
                script_builder.write_body(sql_text)
            script_builder.write_body(f"COMMIT;\n")

    def verify_baseline_scripts(self, script_builder: UpdateScriptBuilder | None) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        baseline_dir = scripts_dir.joinpath(BASELINE_DIR_NAME)
        if not baseline_dir.exists():
            (self.__dict__.get('out') or Output()).print(
                _("The scripts directory '{scripts_dir}' is missing "
                "'{baseline_dir_name}' subdirectory. Baseline scripts "
                "will be skipped.")
                .format(
                    scripts_dir=scripts_dir,
                    baseline_dir_name=BASELINE_DIR_NAME
                )
            )
            return
        
        if self.check_if_version_table_include_baseline_version():
            installed_baseline_version = self.get_baseline_version_installed()
            (self.__dict__.get('out') or Output()).print(
                _("The target schema has the baseline version installed: "
                "{installed_baseline_version}")
                .format(installed_baseline_version=installed_baseline_version)
            )
            return
        baseline_subdirs = [item for item in baseline_dir.iterdir() if item.is_dir()]
        if (baseline_subdirs_len := len(baseline_subdirs)) != 1:
            raise CommandError(
                _("The baseline path {baseline_dir} must have single subdirectory "
                "with the baseline scripts but {baseline_subdirs_len} was found")
                .format(
                    baseline_dir=baseline_dir,
                    baseline_subdirs_len=baseline_subdirs_len
                )
            )
        baseline_version_subdir = baseline_subdirs[0]
        baseline_version = baseline_version_subdir.name

        scripts_sorted = self.get_sorted_scripts_from_dir(baseline_version_subdir, BASELINE_FILES_DEPTH)
        (self.__dict__.get('out') or Output()).print(_("Baseline scripts to install: "))
        self.display_required_changes_by_path(
            scripts_dir, scripts_sorted, version=baseline_version, scripts_table="versioned"
        )

        if script_builder:
            self.write_baseline_scripts(baseline_version, scripts_dir, scripts_sorted, script_builder)

        # remember latest version in scripts for the further use in verify_repeatable()
        self.latest_version_in_scripts = baseline_version

    def write_versioned_scripts(self, version : str, scripts_dir: Path, scripts: list[Path], script_builder: UpdateScriptBuilder) -> None:
        encoding = self.file_read_encoding 
        errors = self.file_read_encoding_errors 
        script_infos = [ 
            ScriptFsInfo.get_info_with_text(
                scripts_dir, s, encoding=encoding, encoding_errors=errors) for s in scripts
        ]
        with script_builder:            
            sql_comment = self.format_sql_comment(f"-- --------- VERSION: {version} ---------")
            script_builder.write_body(sql_comment)
            script_builder.write_body(f"\nBEGIN;\n")
            for i in script_infos:
                sql_comment = self.format_sql_comment(f"-- Apply script: [{i.relative_path} (OID:{i.oid:.8})]")
                script_builder.write_body(sql_comment)
                script_builder.write_body_lines(i.text)
                script_builder.write_body(f"\n-- End of script.\n")
            schema_id = self.get_schema_name()
            sql_text = self.format_sql_text(
                "INSERT INTO {schema_name}.dbmigration_versions (version_id, is_baseline) VALUES ({version_id}, FALSE);\n", 
                schema_name=schema_id, version_id=version)
            script_builder.write_body(sql_text)
            for i in script_infos:
                sql_text = self.format_sql_text(
                    "INSERT INTO {schema_name}.dbmigration_version_scripts (version_id, relative_path, git_blob_sha1) VALUES ({version_id}, {relative_path},{git_blob_sha1});\n", 
                    schema_name=schema_id, version_id=version,relative_path=i.relative_path,git_blob_sha1=i.oid)
                script_builder.write_body(sql_text)
            script_builder.write_body(f"COMMIT;\n")

    def verify_versioned_scripts(self, script_builder: UpdateScriptBuilder | None) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
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
        
        versioned_subdirs = [item for item in versioned_dir.iterdir() if item.is_dir()]
        if not versioned_subdirs:
            raise CommandError(
                _("Versioned scripts path {versioned_dir} must contain at least "
                "one subdirectory, but none were found")
                .format(versioned_dir=versioned_dir)
            )

        latest_installed_version = self.get_latest_version_installed()
        if latest_installed_version is not None:
            newer_version_subdirs = [item for item in versioned_subdirs if item.name > latest_installed_version]
        else:
            newer_version_subdirs = versioned_subdirs

        if not newer_version_subdirs:
            (self.__dict__.get('out') or Output()).print(
                _("The latest installed version is {latest_installed_version}. "
                "No newer scripts found for installation.")
                .format(latest_installed_version=latest_installed_version)
            )       
            return
        
        newer_version_subdirs_sorted = sorted(newer_version_subdirs)
        
        # remember latest version in scripts for the further use in verify_repeatable()
        latest_version = newer_version_subdirs_sorted[-1].name

        if self.latest_version_in_scripts is not None and latest_version <= self.latest_version_in_scripts:
            raise CommandError(
                _("The latest script version '{latest_version}' must be greater "
                "than the baseline script version '{baseline_version_in_scripts}'.")
                .format(
                    latest_version=latest_version,
                    baseline_version_in_scripts=self.latest_version_in_scripts
                )
            )

        self.latest_version_in_scripts = latest_version
    
        (self.__dict__.get('out') or Output()).print(_("Versioned scripts to install: "))    
        for version_dir in newer_version_subdirs_sorted:    
            scripts_sorted = self.get_sorted_scripts_from_dir(version_dir, VERSIONED_FILES_DEPTH)
            if not scripts_sorted:
                filters_str = ",".join(self.file_glob_filters)
                raise CommandError(
                    _("The scripts subdirectory '{version_dir}' does not contain "
                    "any '{filters_str}' scripts.")
                    .format(version_dir=version_dir, filters_str=filters_str)
                )
            version_id = version_dir.name
            self.display_required_changes_by_path(
                scripts_dir, scripts_sorted, version=version_id, scripts_table="versioned"
            )
            if script_builder:
                self.write_versioned_scripts(version_id, scripts_dir, scripts_sorted, script_builder)

    def write_repeatable_scripts(self, target_version: str, script_info_with_text_list: list[ScriptFsInfo], script_builder: UpdateScriptBuilder) -> None:
        with script_builder:
            sql_comment = self.format_sql_comment(f"-- --------- REPEATABLE SCRIPTS FOR VERSION: {target_version} ---------")
            script_builder.write_body(sql_comment)
            schema_id = self.get_schema_name()
            for i in script_info_with_text_list:
                script_builder.write_body(f"\nBEGIN;\n")
                sql_comment = self.format_sql_comment(f"-- Apply script: [{i.relative_path} (OID:{i.oid:.8})]")
                script_builder.write_body(sql_comment) 
                if not i.text:
                    ValueError(f"The text property of script info must not be empty string")
                script_builder.write_body_lines(i.text)
                script_builder.write_body(f"\n")
                script_builder.write_body("-- End of script.\n")  
                sql_text = self.format_sql_text(
                    "INSERT INTO {schema_name}.dbmigration_repeatable_scripts (git_blob_sha1, version_id, relative_path) VALUES ({git_blob_sha1}, {version_id}, {relative_path});\n", 
                    schema_name=schema_id, git_blob_sha1=i.oid, version_id=target_version, relative_path=str(i.relative_path))
                script_builder.write_body(sql_text)
                script_builder.write_body(f"COMMIT;\n")

    def verify_repeatable_scripts(self, script_builder: UpdateScriptBuilder | None) -> None:
        scripts_dir = self.get_resolved_scripts_dir()
        repeatable_dir = scripts_dir.joinpath(REPEATABLE_DIR_NAME)
        if not repeatable_dir.exists():
            (self.__dict__.get('out') or Output()).print(
                _("The scripts directory '{scripts_dir}' is missing "
                "'{repeatable_dir_name}' subdirectory. Repeatable scrips "
                "will be skipped.")
                .format(
                    scripts_dir=scripts_dir,
                    repeatable_dir_name=REPEATABLE_DIR_NAME
                )
            )
            return

        target_version_file_path = repeatable_dir.joinpath(TARGET_VERSION_FILE)
        if not target_version_file_path.exists():
            raise CommandError(
                _("The target version file '{target_version_file}' does not "
                "exist in the repeatable scripts subdirectory '{repeatable_dir}'.")
                .format(
                    target_version_file=TARGET_VERSION_FILE,
                    repeatable_dir=repeatable_dir
                )
            )

        target_version = read_as_trimmed_string(target_version_file_path)

        latest_installed_version = self.get_latest_version_installed()
        if latest_installed_version is None:
           (self.__dict__.get('out') or Output()).print(_("No versions are installed in the database schema.")) 

        self.cross_check_of_the_target_version_for_repeatable_scripts(target_version, self.latest_version_in_scripts, latest_installed_version)

        (self.__dict__.get('out') or Output()).print(
            _("Target version for repeatable scripts: '{target_version}'.")
            .format(target_version=target_version)
        )

        repeatable_scripts_sorted = self.get_sorted_scripts_from_dir(repeatable_dir, REPEATABLE_FILES_DEPTH)

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
        (self.__dict__.get('out') or Output()).print(_("Repeatable scripts to (re)install: "))
        self.display_required_changes(
            script_infos, version=target_version, scripts_table="repeatable"
        )

        if script_builder:
            self.write_repeatable_scripts(target_version, script_infos, script_builder)

    def _run(self) -> int:
        self.make_dbconn_session_readonly()
        self.do_initial_cross_checks()        
        self.check_if_all_own_migrations_are_applied()
        self.check_if_all_version_control_tables_exist()
        self.check_if_stored_environment_id_matches_to_scripts_dir() 
        self.check_if_max_version_of_versioned_scripts_matches_repeatable_target()

        self.git = None
        if not self.opts.skip_git_checks:
            scripts_dir = self.get_resolved_scripts_dir()
            self.git = GitChecker.try_get(self.config, scripts_dir, out=self.out)
        if not self.opts.skip_diffs and self.git is None:
            (self.__dict__.get('out') or Output()).print(
                _("Warning: Diff display requires a Git repository and the Git command line. "
                  "Diff display is disabled.")
            )

        script_builder = None
        script_path = self.opts.build_update_script
        if self.opts.build_update_script is not None:
            script_builder = UpdateScriptBuilder(script_path, out=self.out)
            script_builder.check() 
        try:            
            if script_builder is not None:
                search_path = self.get_search_path_for_scripts()
                self.write_search_path(search_path, script_builder)
            self.verify_baseline_scripts(script_builder)
            self.verify_versioned_scripts(script_builder)
            self.verify_repeatable_scripts(script_builder)
            # finalize writing update script
            if script_builder is not None:
                written = script_builder.get_written_body_bytes()
                if written > 0:
                    script_builder.finalize()
                    (self.__dict__.get('out') or Output()).print(
                        _("Update script is written to '{script_path}'.")
                        .format(script_path=script_path)
                    )
                else:
                    script_builder.cleanup()
                    (self.__dict__.get('out') or Output()).print(
                        _("No updates to write for script '{script_path}'. Temp file cleaned up")
                        .format(script_path=script_path)
                    )

        except Exception:
            if script_builder is not None:
                script_builder.cleanup()
            raise
        if not self.opts.skip_display_recent_changes:
            self.display_recent_changes(RECENT_CHANGES_LIMIT, RECENT_CHANGES_WINDOW_MINUTES)
        return 0
