import os

import pytest

from ddataflow import DDataflow


def test_initialize_successfully():
    """
    Tests that a correct config will not fail to be instantiated
    """
    from ddataflow.samples.ddataflow_config import config

    DDataflow(**config)


def test_wrong_config_fails():
    with pytest.raises(BaseException, match="wrong param"):
        from ddataflow.samples.ddataflow_config import config

        DDataflow(**{**config, **{"a wrong param": "a wrong value"}})


def test_current_project_path():
    """
    Test that varying our environment we get different paths
    """
    config = {
        "project_folder_name": "my_tests",
    }
    ddataflow = DDataflow(**config)
    # by default do not override
    assert ddataflow._get_overriden_arctifacts_current_path() is None
    ddataflow.enable()
    assert (
        "dbfs:/ddataflow/my_tests" == ddataflow._get_overriden_arctifacts_current_path()
    )
    ddataflow.enable_offline()
    assert (
        os.getenv("HOME") + "/.ddataflow/my_tests"
        == ddataflow._get_overriden_arctifacts_current_path()
    )


def test_temp_table_name():

    config = {
        "sources_with_default_sampling": ["location"],
        "project_folder_name": "unit_tests",
    }

    ddataflow = DDataflow(**config)
    ddataflow.disable()
    # by default do not override
    assert ddataflow.name("location", disable_view_creation=True) == "location"
    ddataflow.enable()
    assert (
        ddataflow.name("location", disable_view_creation=True) == "unit_tests_location"
    )

def test_temp_table_name_backwards_compatability_dwh_schema():

    config = {
        "sources_with_default_sampling": ["dwh.location"],
        "project_folder_name": "unit_tests",
    }

    ddataflow = DDataflow(**config)
    ddataflow.disable()
    # by default do not override
    assert ddataflow.name("dwh.location", disable_view_creation=True) == "dwh.location"
    ddataflow.enable()
    assert (
        ddataflow.name("dwh.location", disable_view_creation=True) == "unit_tests_location"
    )

def test_temp_table_name_backwards_compatability_non_dwh_schema():

    config = {
        "sources_with_default_sampling": ["default.events"],
        "project_folder_name": "unit_tests",
    }

    ddataflow = DDataflow(**config)
    ddataflow.disable()
    # by default do not override
    assert ddataflow.name("default.events", disable_view_creation=True) == "default.events"
    ddataflow.enable()
    assert (
        ddataflow.name("default.events", disable_view_creation=True) == "unit_tests_default.events"
    )

def test_temp_table_name_with_generating_source_name():
    def create_temp_table_name(name: str, project_folder_name: str) -> str:
        return f"{project_folder_name}-{name.replace('.', '__')}"

    config = {
        "sources_with_default_sampling": ["dwh.location"],
        "project_folder_name": "unit_tests",
        "generate_source_name": create_temp_table_name,
    }

    ddataflow = DDataflow(**config)
    ddataflow.disable()
    # by default do not override
    assert ddataflow.name("dwh.location", disable_view_creation=True) == "dwh.location"
    ddataflow.enable()
    assert (
        ddataflow.name("dwh.location", disable_view_creation=True) == "unit_tests-dwh__location"
    )
