# Phase 2: Location Estimation of a Tweet

Before running the execution, place the geotweets.tsv file in the data folder.

## Code Execution

**Run command:**

```sh
$ cd PhaseTwo
$ SPARK-LOCATION\bin\spark-submit classify.py -t data/geotweets.tsv -i data/input1.txt -o data/output.tsv
```

**Execution results** can be found [here](https://github.com/FredrikBakken/TDT4305_Big-Data-Project/blob/master/PhaseTwo/data/output.tsv).

## Known Issues

During the testing we noticed an error message on one of our laptops, which was a TypeError:

```sh
TypeError: a bytes-like object is required, not 'str'
```

An alternative solution to this issue is to set "use_unicode=True" on line 90 in classify.py.