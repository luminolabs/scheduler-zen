import os
import pytest
from unittest.mock import patch
import io
import yaml

from app.config_manager import ConfigManager, is_truthy, is_falsy


@pytest.fixture
def mock_env():
    """
    Fixture to set up a mock environment for testing.

    This fixture sets up a controlled environment with predefined
    environment variables for testing purposes.
    """
    with patch.dict(os.environ, {
        'SZ_CONF_PATH': '/test/path',
        'SZ_ENV': 'test',
        'SZ_TEST_VAR': 'test_value',
        'SZ_BOOLEAN_TRUE': 'true',
        'SZ_BOOLEAN_FALSE': 'false'
    }, clear=True):
        yield


def test_load(mock_env):
    """
    Test the load method of ConfigManager.

    Verifies that the configuration is correctly loaded from YAML files
    and environment variables, with proper overriding behavior.
    """
    default_yaml = """
    db_name: default_db
    db_user: default_user
    db_pass: default_pass
    test_var: default_test_value
    boolean_true: 
    boolean_false: 
    """
    test_yaml = """
    db_name: test_db
    db_user: test_user
    """

    mock_files = {
        '/test/path/default.yml': default_yaml,
        '/test/path/test.yml': test_yaml
    }

    def mock_open_func(filename, *args, **kwargs):
        if filename in mock_files:
            return io.StringIO(mock_files[filename])
        raise FileNotFoundError(f"(mock) Unable to open {filename}")

    with patch('builtins.open', mock_open_func), \
            patch('os.path.exists', lambda path: path in mock_files), \
            patch('yaml.safe_load', side_effect=yaml.safe_load) as mock_yaml_load, \
            patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:

        config = ConfigManager()
        loaded_config = config.loaded_config

        # Print captured stdout
        print(f"Captured stdout:\n{mock_stdout.getvalue()}")

        print(f"Loaded config: {loaded_config}")  # Debug print

    assert 'db_name' in loaded_config, f"db_name not in loaded_config. Keys: {loaded_config.keys()}"
    assert loaded_config['db_name'] == 'test_db'
    assert loaded_config['db_user'] == 'test_user'
    assert loaded_config['db_pass'] == 'default_pass'
    assert loaded_config['test_var'] == 'test_value'
    assert loaded_config['boolean_true'] is True
    assert loaded_config['boolean_false'] is False


def test_getattr(mock_env):
    """
    Test the __getattr__ method of ConfigManager.

    Verifies that configuration values can be accessed as attributes
    of the ConfigManager instance.
    """
    with patch.object(ConfigManager, 'load', return_value={
        'db_name': 'test_db',
        'db_user': 'test_user',
        'db_pass': 'test_pass',
        'test_var': 'test_value'
    }):
        config = ConfigManager()
        assert config.db_name == 'test_db'
        assert config.db_user == 'test_user'
        assert config.db_pass == 'test_pass'
        assert config.test_var == 'test_value'


def test_database_url(mock_env):
    """
    Test the construction of the database URL.

    Verifies that the database_url property is correctly constructed
    from the individual database configuration parameters.
    """
    with patch.object(ConfigManager, 'load', return_value={
        'db_name': 'test_db',
        'db_user': 'test_user',
        'db_pass': 'test_pass',
        'db_host': 'test_host',
        'db_port': '5432'
    }):
        config = ConfigManager()
        expected_url = "postgresql://test_user:test_pass@test_host:5432/test_db"
        assert config.database_url == expected_url


def test_is_truthy():
    """
    Test the is_truthy function.

    Verifies that the function correctly identifies truthy values.
    """
    assert is_truthy(True)
    assert is_truthy('true')
    assert is_truthy('1')
    assert is_truthy('yes')
    assert not is_truthy(False)
    assert not is_truthy('false')
    assert not is_truthy('0')
    assert not is_truthy('no')


def test_is_falsy():
    """
    Test the is_falsy function.

    Verifies that the function correctly identifies falsy values.
    """
    assert is_falsy(False)
    assert is_falsy('false')
    assert is_falsy('0')
    assert is_falsy('no')
    assert not is_falsy(True)
    assert not is_falsy('true')
    assert not is_falsy('1')
    assert not is_falsy('yes')
