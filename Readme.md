UGS Water Chemistry Database Seeder
===================================

[![Build Status](https://travis-ci.org/agrc/ugs-db.svg?branch=version2)](https://travis-ci.org/agrc/ugs-db) [![codecov.io](http://codecov.io/github/agrc/ugs-db/coverage.svg?branch=version2)](http://codecov.io/github/agrc/ugs-db?branch=master)

A db tool for seeding and updating the ugs water chemistry database.

### Setup
1. Install oracle instant client >= 19.3.0.0.0
    1. Download the "Basic Light Package" from [this page](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html#ic_winx64_inst).
    1. Follow [install directions](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html#ic_winx64_inst) at the bottom of the download page.
        - No need to mess with co-locating optional oracle config files such as `tnsnames.ora`.
    1. Follow [ODBC installation instructions](https://www.oracle.com/database/technologies/releasenote-odbc-ic.html) for windows.
1. Update `ugssecrets.py` based on the [sample.](/src/dbseeder/ugssecrets_sample.py)
    1. To find the name of your instant client driver: `import pyodbc;pyodbc.drivers()`.
1. execute `scripts\createDB.sql` or create a sql server database called `UGSWaterChemistry`
1. execute `python -m ugsdbseeder create-tables <configuration>` to create db tables.
1. see usage (`python -m ugsdbseeder -h`)

### Usage
from the `**/src**` directory  
execute `python -m ugsdbseeder -h` for usage.

### Tests
from the **parent** project directory  
`tox`
