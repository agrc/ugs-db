# dbseeder

### installing [oracle client](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html)

1. [Instant Client Package](http://download.oracle.com/otn/nt/instantclient/121010/instantclient-basic-windows.x64-12.1.0.1.0.zip)
1. Create a folder on disk and extract to that location
1. Add location to windows path
1. Install [cx_Oracle](https://pypi.python.org/pypi/cx_Oracle/5.1.3)
1. Install [PyProj](https://code.google.com/p/pyproj/downloads/list)

### About

This package will take UGS, SDWIS, DOGM, WQP, and UDWR data and import it into a gdb.  

## Installation

`setup.py install`

### Usage

1. Fill out `secrets.cfg` and use `secrets.sample.cfg` as an example
1. [Download the data](https://drive.google.com/a/utah.gov/file/d/0BzQ5lAz8kBCAMkZPZ1c3S21mUHc/edit?usp=sharing)
1. Place it in `dbseeder\data` by default or pass in the `--data` flag 
    1. This assumes you are cd'd into `dbseeder/src`  
    
`python -m dbseeder --seed` will create the **gdb**, the **stations** point feature class and the **results** table.

For **WQP**, the module will look for all `*.csv's` in `data\WQP\Result` and `data\WQP\Station` folders.  
For **GDB** based programs, the module will look for `data\Program Name\Program.gdb\Station or Result` and import from the tables.  
For **SDWIS** the module will query the database and import the rows. 

Once that is done you can create the relationship feature class by running

`python -m dbseeder --relate`

`python -m dbseeder --update` is still a work in progress but all the plumbing is there. We just need to figure out how to get the query to the program and what that query should be.

## Tests

1. [Download the test data](). 
1. Place it in `dbseeder\tests\data`
1. Install tox
    1. `pip install tox`
1. Run tests
    1. `tox`

## Profiling
run `dbseeder.py` this will create a `.pstat` file  
execute `python -m pstats .pstat` to enter the pstat browser  
enter `strip` to remove long paths  
enter `sort time`  to sort the stats by time executing  
enter `stats #` to print # number of records  

to create a qcachegrind file  
`pyprof2calltree -i .pstat -o callgrind.pstat`  
install and run qcachegrind  
`qcachegrind`  
open file