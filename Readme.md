WRI ETL
========

A db seeder etl tool for wri data.

### Setup
1. execute `scripts\create_db.sql`
1. execute `scripts\create_tables.sql`
1. enable SDE
1. register tables with database
1. see usage

### Model Schema
- The key values are the source table attributes
- the values contain the information about the etl process
    - type is the field type. Used for casting
    - map is the destination field
    - order is to sync the order for querying, inserting, and updating
- fields prefixed with `*` are not found in the source and are expected to have a `value` property to insert into that field on the destintation table.
- fields prefeixed with `!` are duplicated where you need to act on the same field twice

### Usage
from the **/src** directory

`python -m dbseeder seed "connections\WRI on (production).sde" "connections\WRI_Spatial on (local).sde"`

`python -m dbseeder seed "connections\WRI on (local).sde" "connections\WRI_Spatial on (local).sde"`

### Tests
from the **parent** project directory
`tox`

### Problems
`expecting string data` means the lookup value was not in the models table. Change batch size to 2 and look for a number where there should be a value. Add the number: None

`string or binary data to be truncated` - run `python -m dbseeder path/to/csv's --length` and adjust sql schema 
