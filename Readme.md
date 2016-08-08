UGS Water Chemistry Database Seeder
===================================

[![Build Status](https://travis-ci.org/agrc/ugs-db.svg?branch=version2)](https://travis-ci.org/agrc/ugs-db) [![codecov.io](http://codecov.io/github/agrc/ugs-db/coverage.svg?branch=version2)](http://codecov.io/github/agrc/ugs-db?branch=master)

A db tool for seeding and updating the ugs water chemistry database.

### Setup
1. Install oracle client >= 11.2
    1. Use the Oracle Net Manager to create a Service Name called `sdwis`
        1. **Protocol**: `TCP/IP`
        1. **Host**: `<db host>`
        1. **Service Name**: `env`
        1. **Port**: `1521`
1. Update `ugssecrets.py` based on the [sample.](/src/dbseeder/ugssecrets_sample.py)
    1. Use `C:\Windows\System32\odbcad32.exe` to find your 64 bit oracle driver if you are using 64 bit python.
    1. Use `C:\Windows\SysWOW64\odbcad32.exe` to find your 32 bit oracle driver if you are using 32 bit python.
![image](https://cloud.githubusercontent.com/assets/325813/11985072/685e4382-a97e-11e5-9dbc-24f811ec3ce5.png)
1. execute `scripts\createDB.sql` or create a sql server database called `UGSWaterChemistry`
1. execute `python -m ugsdbseeder create-tables <configuration>` to create db tables.
1. see usage (`python -m ugsdbseeder -h`)

### Usage
from the `**/src**` directory  
execute `python -m ugsdbseeder -h` for usage.

### Tests
from the **parent** project directory  
`tox`
