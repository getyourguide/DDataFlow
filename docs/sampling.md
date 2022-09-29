## Sampling on the notebook

Add the following to your setup.py

```py
    py_modules=[
        "ddataflow_config",
    ],
```

## With Dbrocket

Cell 1

```sh
%pip install --upgrade pip 
%pip install ddataflow
%pip install /dbfs/temp/jean.machado/search_ranking_pipeline-1.0.1-py3-none-any.whl --force-reinstall`
```

Cell 2

```py
from ddataflow_config import ddataflow
ddataflow.save_sampled_data_sources(ask_confirmation=False)

```
