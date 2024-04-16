
One drawback of having ddataflow in the root folder is that it can conflict with other ddtaflow installations.
Prefer installing ddataflow in submodules of your main project (`myproject/main_module/ddataflow_config.py`) instead of globally (`myproject/ddataflow_config.py`).
