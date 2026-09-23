from __future__ import annotations

import difflib
import hashlib
import mmap
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Self

from _constants import SCRIPT_LIST_FILE_NAME
from _errors import CommandError
from _i18n import _



def get_git_blob_sha1_for_bytes(script_bytes : bytes) -> str:
    content = script_bytes.replace(b'\r\n', b'\n')
    header = f"blob {len(content)}\x00".encode('utf-8')
    sha1 = hashlib.sha1(header)
    sha1.update(content)
    result = sha1.hexdigest()
    return result

def get_git_blob_sha1_for_file_path(file_path: str | Path) -> str:
    path = Path(file_path)
    chunk_size = 1048576  

    def bytes_stream():
        if path.stat().st_size == 0:
            return                
        with open(path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                while chunk := mm.read(chunk_size):
                    yield chunk.replace(b"\r", b"")

    total_bytes = sum(len(chunk) for chunk in bytes_stream())

    header = f"blob {total_bytes}\x00".encode("utf-8")
    sha1 = hashlib.sha1(header)
    
    for chunk in bytes_stream():
        sha1.update(chunk)
        
    result = sha1.hexdigest()
    return result

def get_script_path_for_log(scripts_dir: str|Path, script_path: str|Path) -> str:
    base_dir = Path(scripts_dir).parent.resolve()
    target_file = Path(script_path).resolve()
    if target_file.is_relative_to(base_dir):
        result = target_file.relative_to(base_dir).as_posix()
    else:
        result = target_file.as_posix()
    return result

def render_script_diff_text(
    old_text: str,
    new_text: str,
    relative_path: str,
    old_oid: str | None,
    new_oid: str,
) -> str:
    """Renders a unified text diff between the applied (DB) and the repo versions of a script."""
    old_oid_label = old_oid[:8] if old_oid else _("new script")
    from_label = _("a/{relative_path} (DB OID: {old_oid})").format(
        relative_path=relative_path, old_oid=old_oid_label
    )
    to_label = _("b/{relative_path} (REPO OID: {new_oid})").format(
        relative_path=relative_path, new_oid=new_oid[:8]
    )
    diff_lines = difflib.unified_diff(
        old_text.splitlines(),
        new_text.splitlines(),
        fromfile=from_label,
        tofile=to_label,
        lineterm="",
        n=3,
    )
    return "\n".join(diff_lines)

def read_as_trimmed_string(file_path : str|Path) -> str:
    with open(file_path, 'rb') as f:
        for binary_line in f:
            decoded_str = binary_line.decode("utf-8-sig", "ignore")
            trimmed_str = decoded_str.strip()
            if trimmed_str:
                return trimmed_str
    raise CommandError(_("The file '{file_path}' contains no valid text data").format(file_path=file_path))

def resolve_relative_script_path(start_path: Path, depth_within_base_dir: int, path_str : str) -> Path:
    if not path_str.startswith("@"):
        raise CommandError(
            _("The relative environment path must start with @ symbol, but '{path_str}' was found")
            .format(path_str=path_str)
        )
    # path normalization for windows style paths
    path_str = path_str.replace("\\", "/")
    start = path_str.find("@") + 1
    end = path_str.find("/", start)
    if end == -1:
        script_list_file_path = start_path.joinpath(SCRIPT_LIST_FILE_NAME)
        raise CommandError(
            _(
                "No path separator found after environment name in path '{path_str}' "
                "specified in file '{script_list_file_path}'."
            ).format(path_str=path_str, script_list_file_path=script_list_file_path)
        )
    env_name = path_str[start:end]
    script_sub_path = path_str[end + 1:]
    result = start_path
    # walk back
    for i in range(depth_within_base_dir + 1):
        result = result.joinpath("..")
    # add a referencing env name
    result = result.joinpath(env_name)
    # walk forward
    last_parts = start_path.parts[-depth_within_base_dir:] if depth_within_base_dir > 0 else ()
    for part in last_parts:
        result = result.joinpath(part)
    # add extra path specified after env name
    result = result.joinpath(script_sub_path)
    return result

class ScriptFsInfo(NamedTuple):
    script_path : Path
    relative_path : str
    oid : str
    text : str

    def __repr__(self) -> str:
        short_oid = self.oid[:8]
        return f"[{self.relative_path} (OID: {short_oid})]"

    @classmethod
    def get_info(
        cls, 
        scripts_dir: str | Path, 
        script_path: str | Path
    ) -> Self:
        """
        Factory method for potentially large files. 
        Calculates Git SHA-1 efficiently in chunks without loading text into memory.
        """
        relative_script_path = get_script_path_for_log(scripts_dir, script_path)
        git_blob_sha1 = get_git_blob_sha1_for_file_path(script_path)
        
        result = cls(
            script_path=script_path, 
            relative_path=relative_script_path,
            oid=git_blob_sha1,
            text=""
        )
        return result 

    @classmethod
    def get_info_with_text(
        cls, 
        scripts_dir: str | Path, 
        script_path: str | Path, 
        encoding: str = "utf-8-sig", 
        encoding_errors: str = "ignore"
    ) -> Self:
        """
        Factory method for small/medium files where full text content is needed.
        Loads file bytes to calculate SHA-1 and decodes them into the 'text' field.
        """
        relative_script_path = get_script_path_for_log(scripts_dir, script_path)
        
        with open(script_path, 'rb') as f:
            script_bytes = f.read()
            
        git_blob_sha1 = get_git_blob_sha1_for_bytes(script_bytes)
        text = script_bytes.decode(encoding, encoding_errors)
        
        result = cls(
            script_path=script_path, 
            relative_path=relative_script_path,
            oid=git_blob_sha1,
            text=text
        )
        return result

class ScriptDbInfo(NamedTuple):
    applied_at : datetime
    script_type : str
    version_id : str
    relative_path : str
    git_blob_sha1 : str
    def __repr__(self) -> str:
        date_str = self.applied_at.strftime("%Y-%m-%d %H:%M:%S")
        clean_oid = self.git_blob_sha1.strip()[:8]
        return f"  [{date_str} | {self.script_type:<10} | {self.version_id:<6} | {self.relative_path} (OID: {clean_oid})]"
