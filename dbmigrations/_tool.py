"""ExternalTool integration (reads TOOL_* constants via the dbmigration module at call time)."""

import locale
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Self

dbmigration = sys.modules.get("dbmigration") or sys.modules.get("__main__")

from _constants import TOML_CONFIG_FILE, TOOLS_CONFIG_GROUP, USE_TOOL_NAME_FILE_NAME
from _errors import CommandError
from _i18n import _
from _output import Output
from _scripts import read_as_trimmed_string


class ExternalTool:
    def __init__(
        self, 
        tool_name: str, 
        schema_name: str, 
        dbconn_config: Dict[str, Any], 
        tool_config: Dict[str, Any]
    ) -> None:
        """Initializes the external tool configuration and caches system encoding."""
        self.tool_name = tool_name
        self.schema_name = schema_name
        self.dbconn_config = dbconn_config
        self.tool_config = tool_config
        
        # Detect and cache system encoding once to save CPU cycles on multiple runs
        self.system_encoding = locale.getpreferredencoding(False)

        # Read tool config
        if dbmigration.TOOL_EXEC_ATTRIBUTE not in tool_config:
            raise CommandError(
                _(
                    "Missing required attribute '{tool_exec_attribute}' "
                    "in tool configuration '{tool_name}'."
                ).format(tool_exec_attribute=dbmigration.TOOL_EXEC_ATTRIBUTE, tool_name=tool_name)
            )
        exec_attribute = tool_config[dbmigration.TOOL_EXEC_ATTRIBUTE]
        exec_path = Path(exec_attribute)
        if not exec_path.exists():
            raise CommandError(
                _(
                    "The path '{exec_path}' specified by attribute '{tool_exec_attribute}' "
                    "in the tool configuration '{tool_name}' does not exists."
                ).format(
                    exec_path=exec_path,
                    tool_exec_attribute=dbmigration.TOOL_EXEC_ATTRIBUTE,
                    tool_name=tool_name,
                )
            )
        if not exec_path.is_file():
            raise CommandError(
                _(
                    "The path '{exec_path}' specified by attribute '{tool_exec_attribute}' "
                    "in the tool configuration '{tool_name}' is not a file."
                ).format(
                    exec_path=exec_path,
                    tool_exec_attribute=dbmigration.TOOL_EXEC_ATTRIBUTE,
                    tool_name=tool_name,
                )
            )
        self.exec_path = exec_path

        if dbmigration.TOOL_ARGS_ATTRIBUTE not in tool_config:
            raise CommandError(
                _(
                    "There is no attribute '{tool_args_attribute}' "
                    "in the tool configuration '{tool_name}'."
                ).format(tool_args_attribute=dbmigration.TOOL_ARGS_ATTRIBUTE, tool_name=tool_name)
            )
        self.args = tool_config[dbmigration.TOOL_ARGS_ATTRIBUTE]

        if dbmigration.TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE not in tool_config:
            raise CommandError(
                _(
                    "There is no attribute '{tool_success_result_code_attribute}' "
                    "in the tool configuration '{tool_name}'."
                ).format(
                    tool_success_result_code_attribute=dbmigration.TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE,
                    tool_name=tool_name,
                )
            )
        self.success_result_code = tool_config[dbmigration.TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE]

    @classmethod
    def try_get(
        cls, 
        dir : Path, 
        schema_name : str, 
        dbconn_config : dict[str, Any], 
        toml_config : dict[str, Any]
    ) -> Self | None:
        tool_name = ExternalTool._try_get_tool_name(dir)
        if tool_name is None:
            return None

        if TOOLS_CONFIG_GROUP not in toml_config:
            raise CommandError(
                _(
                    "Missing configuration group '{tools_config_group}' "
                    "in configuration file '{toml_config_file}'."
                ).format(
                    tools_config_group=TOOLS_CONFIG_GROUP,
                    toml_config_file=TOML_CONFIG_FILE,
                )
            )
        tools_config = toml_config[TOOLS_CONFIG_GROUP]                
        if tool_name not in tools_config:
            raise CommandError(
                _(
                    "Unable find the specified external tool name '{tool_name}' "
                    "in configuration group '{tools_config_group}'."
                ).format(tool_name=tool_name, tools_config_group=TOOLS_CONFIG_GROUP)
            )
        tool_config = tools_config[tool_name]
        
        result = cls(
            tool_name, schema_name, dbconn_config, tool_config
        )
        return result        

    @classmethod
    def _try_get_tool_name(cls, dir : Path) -> str|None:
        start_path = Path(dir) 
        if not start_path.exists():
            raise CommandError(
                _("The folder '{dir}' does not exists").format(dir=dir)
            )
        if not start_path.is_dir():
            raise CommandError(
                _("The path '{dir}' is not a directory").format(dir=dir)
            )       
        use_tool_file_name = start_path.joinpath(USE_TOOL_NAME_FILE_NAME)
        if not use_tool_file_name.exists():
            return None
        tool_name = read_as_trimmed_string(use_tool_file_name)
        return tool_name        

    def make_variables_dict_from_config_and_script_path(self, script_path: str) -> Dict[str, Any]:
        """Creates a token lookup dictionary for variable substitution."""
        result = {}
        for key, value in self.dbconn_config.items():
            variable_key = "${" + key.strip() + "}"  
            result[variable_key] = value
        result["${file}"] = script_path
        result["${schema_name}"] = self.schema_name
        return result

    def match_variables_to_args(self, variables: Dict[str, Any], args: List[str]) -> List[str]:
        """Maps argument placeholders to their actual python-internal unicode string values."""
        result = []
        for arg in args:
            variable_key = arg.strip() 
            if variable_key in variables:
                value_str = str(variables[variable_key])
                result.append(value_str)
            else:
                result.append(arg)
        return result 
    
    def run(self, script_path: str) -> int:
        """Executes the tool in a safe context, streaming its output using system-native encoding."""
        tool_absolute_path = self.exec_path.absolute()
        tool_args = self.args
        variables = self.make_variables_dict_from_config_and_script_path(script_path)
        tool_args_with_matched_variables = self.match_variables_to_args(variables, tool_args)
        command_line = [str(tool_absolute_path), *tool_args_with_matched_variables]
        
        with subprocess.Popen(
            args=command_line, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True,
            encoding=self.system_encoding,
            errors='replace'
        ) as process:
            
            if process.stdout:
                for line in iter(process.stdout.readline, ''):
                    (self.__dict__.get('out') or Output()).print(line, end='') 

            result_code = process.wait() 

        if result_code != self.success_result_code:
            raise CommandError(
                _("The tool '{tool_name}' returned unsuccessful result code {result_code}!")
                .format(tool_name=self.tool_name, result_code=result_code)
            )
            
        return result_code 
