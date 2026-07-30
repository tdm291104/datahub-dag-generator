"""Offline checks for .env loading."""
from __future__ import annotations

import os
from pathlib import Path
import tempfile

from dag_generator.config import load_env_file, load_settings


def test_env_file_does_not_overwrite_shell_value():
    name = "DATAHUB_TEST_SETTING"
    previous = os.environ.pop(name, None)
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(f"export {name}='from-file' # comment\n")
            load_env_file(path)
            assert os.environ[name] == "from-file"
            os.environ[name] = "from-shell"
            load_env_file(path)
            assert os.environ[name] == "from-shell"
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def test_settings_find_env_from_working_directory():
    name = "DATAHUB_SERVER"
    previous_value = os.environ.pop(name, None)
    previous_directory = Path.cwd()
    try:
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            nested = project / "nested"
            nested.mkdir()
            (project / ".env").write_text(f"{name}=http://from-working-directory:8080\n")
            os.chdir(nested)
            assert load_settings().datahub_server == "http://from-working-directory:8080"
    finally:
        os.chdir(previous_directory)
        if previous_value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous_value


if __name__ == "__main__":
    test_env_file_does_not_overwrite_shell_value()
    test_settings_find_env_from_working_directory()
    print("2 .env loading tests passed")
