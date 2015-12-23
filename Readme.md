UGS Water Chemistry Database Seeder
===================================

[![Build Status](https://travis-ci.org/agrc/ugs-db.svg?branch=version2)](https://travis-ci.org/agrc/ugs-db) [![codecov.io](http://codecov.io/github/agrc/ugs-db/coverage.svg?branch=version2)](http://codecov.io/github/agrc/ugs-db?branch=master)

A db tool for seeding and updating the ugs water chemistry database.

### Setup
1. Install oracle client >= 11.2
    1. Use the Oracle Net Manager to create a Service Name called `sdwis`
        1. **Protocol**: `TCP/IP`
        1. **Host**: `itdb208.dts.utah.gov`
        1. **Service Name**: `env`
        1. **Port**: `1521`
1. Update `secrets.py` based on the [sample.](/src/dbseeder/secrets_sample.py)
    1. Use `C:\Windows\System32\odbcad32.exe` to find your 64 bit oracle driver if you are using 64 bit python.
    1. Use `C:\Windows\SysWOW64\odbcad32.exe` to fidn your 32 bit oralce driver if you are using 32 bit python.
![image](https://cloud.githubusercontent.com/assets/325813/11985072/685e4382-a97e-11e5-9dbc-24f811ec3ce5.png)
1. execute `scripts\create_db.sql`
1. see usage

### Usage
from the **/src** directory

execute `python -m dbseeder -h` for usage.

### Tests
from the **parent** project directory
`tox`
