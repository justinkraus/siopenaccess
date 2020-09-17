# SI Open Access AWS Example - Edited for Command Line Python

This is a modification on the original example by the SI Data Science lab to recreate the first part of the jupyter notebook for "Using Dask to parse and filter collections metadata on AWS" in Command Line Python

###Pre-reqs for Python environment

Some additional libraries are needed for Python 3, please run the following commands in terminal to install them:

Dask
```python -m pip install "dask[complete]"```

S3FS
```pip install s3fs```

Numpy
```pip install numpy```

Pandas
```pip install pandas```


###Running the file

Run the file in python by typing

```python3 saam_metadata.py```

This will save an example json record as well as a pickle file of the data repository. Some additional overviews of the datafile will be printed in console.