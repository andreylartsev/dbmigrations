import pathlib
import pytest
from unittest.mock import MagicMock, patch

import dbmigration
from dbmigration import ExternalTool, CommandError


@pytest.fixture
def base_configs():
    dbmigration.TOOLS_CONFIG_GROUP = "tools"
    dbmigration.TOML_CONFIG_FILE = "config.toml"
    dbmigration.TOOL_EXEC_ATTRIBUTE = "exec"
    dbmigration.TOOL_ARGS_ATTRIBUTE = "args"
    dbmigration.TOOL_SUCCESS_RESULT_CODE_ATTRIBUTE = "success_code"

    dbconn_config = {"host": "localhost", "user": "admin"}
    tool_config = {
        "exec": "dummy_path/tool.exe",
        "args": ["${host}", "-u", "${user}", "${file}", "${schema_name}", "static_arg"],
        "success_code": 0
    }
    return dbconn_config, tool_config


@patch("pathlib.Path.is_file", return_value=True)
@patch("pathlib.Path.exists", return_value=True)
def test_tool_initialization_and_variable_matching(mock_exists, mock_is_file, base_configs):
    dbconn_config, tool_config = base_configs
    
    tool = ExternalTool(
        tool_name="my_tool",
        schema_name="public",
        dbconn_config=dbconn_config,
        tool_config=tool_config
    )
    
    assert tool.tool_name == "my_tool"
    assert tool.success_result_code == 0
    
    variables = tool.make_variables_dict_from_config_and_script_path("migration.sql")
    assert variables["${host}"] == "localhost"
    assert variables["${user}"] == "admin"
    assert variables["${file}"] == "migration.sql"
    assert variables["${schema_name}"] == "public"
    
    final_args = tool.match_variables_to_args(variables, tool.args)
    assert final_args == ["localhost", "-u", "admin", "migration.sql", "public", "static_arg"]


def test_init_raises_missing_config_group(base_configs):
    dbconn_config, _ = base_configs
    invalid_toml = {"wrong_group": {}}
    
    with pytest.raises(CommandError) as exc_info:
        ExternalTool("my_tool", "public", dbconn_config, invalid_toml)
    assert "Missing required attribute 'exec'" in str(exc_info.value)


@patch("pathlib.Path.exists", return_value=False)
def test_init_raises_executable_does_not_exist(mock_exists, base_configs):
    dbconn_config, toml_config = base_configs
    
    with pytest.raises(CommandError) as exc_info:
        ExternalTool("my_tool", "public", dbconn_config, toml_config)
    assert "does not exists" in str(exc_info.value)


@patch("pathlib.Path.is_file", return_value=True)
@patch("pathlib.Path.exists", return_value=True)
@patch("subprocess.Popen")
def test_tool_run_success(mock_popen, mock_exists, mock_is_file, base_configs, capsys):
    dbconn_config, toml_config = base_configs
    tool = ExternalTool("my_tool", "public", dbconn_config, toml_config)
    
    mock_process = MagicMock()
    mock_process.returncode = 0
    mock_process.wait.return_value = 0
    
    mock_process.stdout.readline.side_effect = ["Log line 1\n", "Log line 2\n", ""]
    
    mock_popen.return_value.__enter__.return_value = mock_process
    
    exit_code = tool.run("migration.sql")
    
    assert exit_code == 0
    mock_popen.assert_called_once()
    assert mock_popen.call_args[1]["encoding"] == tool.system_encoding
    assert mock_popen.call_args[1]["errors"] == "replace"
    
    captured = capsys.readouterr()
    assert "Log line 1\nLog line 2\n" in captured.out


@patch("pathlib.Path.is_file", return_value=True)
@patch("pathlib.Path.exists", return_value=True)
@patch("subprocess.Popen")
def test_tool_run_unsuccessful_code(mock_popen, mock_exists, mock_is_file, base_configs):
    dbconn_config, tool_config = base_configs
    tool = ExternalTool("my_tool", "public", dbconn_config, tool_config)
    
    mock_process = MagicMock()
    mock_process.wait.return_value = 1
    mock_process.stdout.readline.side_effect = [""]
    mock_popen.return_value.__enter__.return_value = mock_process
    
    with pytest.raises(CommandError) as exc_info:
        tool.run("migration.sql")
    assert "returned unsuccessful result code 1" in str(exc_info.value)
