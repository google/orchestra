"""Utility functions for building DAGs
"""
from pathlib import Path

import yaml
from airflow.models import Variable


def load_config(caller_path, path=None):
    """Load a standard configuration yaml file. The file is opened relative from the
        caller_path.

    Args:
        caller_path (string): Absolute path of the caller file. Typically passed with
            `__file__`.
        path (string, optional): Relative path to the configuration file from
            `caller_path`. Defaults to `config.yaml`.

    Returns:
        Dict: The configuration dictionary.
    """

    caller_wd = Path(caller_path).parent
    if not path:
        path = Variable.get("config_path", "config.yaml")

    with Path(caller_wd, path).open() as file:
        config = yaml.load(file)

    return config


def load_docs(caller_path, path=None):
    """Load a docs file as a string.

    Args:
        caller_path (string): Absolute path of the caller file. Typically passed with
            `__file__`.
        path (string, optional): Relative path to the docs file from
            `caller_path`. Defaults to `README.md`.

    Returns:
        Dict: The docs string.
    """

    caller_wd = Path(caller_path).parent
    if not path:
        path = Variable.get("docs_path", "README.md")

    with Path(caller_wd, path).open() as file:
        docs = file.read()

    return docs
