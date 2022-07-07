# Local development with DDataflow

DDataflow also enables one to develop with local data. We see this though as a more advanced use case, which might be
the first choice for everybody. First, make a copy of the files you need to download in dbfs.

```py
ddataflow.save_sampled_data_sources(ask_confirmation=False)
```

Then in your machine:
```sh
$ ddataflow current_project download_data_sources
```

Now you can use the pipeline locally by exporting the following env variables:

```shell
export ENABLE_OFFLINE_MODE=true
# run your pipeline as normal
python yourproject/train.py
```


The downloaded data sources will be stored at `$HOME/.ddataflow`.
