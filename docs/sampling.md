## Sampling on the notebook

Add the following to your setup.py

```py
    # given that ddataflow config usually sits on the root of the project
    # we add it to the package data manually if we want to access the config 
    # installed as a library
    py_modules=[
        "ddataflow_config",
    ],
```

## With Dbrocket

Cell 1

```sh
%pip install --upgrade pip 
%pip install ddataflow
%pip install /dbfs/temp/user/search_ranking_pipeline-1.0.1-py3-none-any.whl --force-reinstall`
```

Cell 2

```py
from ddataflow_config import ddataflow
ddataflow.save_sampled_data_sources()
```
