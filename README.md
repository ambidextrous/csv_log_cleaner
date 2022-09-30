# CSV Cleaner

## Overview

The purpose CSV Cleaner is to allow users to define a column-wise type schema 
for a CSV file and to clean the rows of the file so that they will conform 
to that schema. It allows users to do this quickly, with a very low memory 
overhead and with logging of all destructive operations carried out during 
the cleaning process.

## Warning

This my first ever Rust project, implemented as a solution to a recurring 
issue I keep coming across in my work as a data engineer. So please don't 
put this thing into production until you've done your own thorough testing.

Also, if you find any problems with the code or improvments that can be made 
please just create a PR or open an issue. The script is literally the result 
of my reading half-way through the Rust book and then fighting the borrow 
checker until the thing would compile. So thanks in advance for ripping my 
code to shreds in the service of teaching me how Rust ought to be written.

## Usage

Example `cargo` command to build and run the tool:

```
cargo run -- -i input_file.csv -o output_file.csv -j csv_schema.json -l log_file.json
```

To build and run tests:

```
cargo test
```

## Motivation

In order to be able to work with CSV data in a database or dataframe, it is 
typically necessary to be able to make some basic assumptions about the data, 
e.g. that an integer field will contain only valid integer values. However, the 
fact that the CSV format does not enforce a data schema beyond a basic 
consistency of the number of columns - combined with the fact that CSV data is 
often generated through error-prone manual processes - means that it is often 
not possible to make such assumptions about CSV data encountered in the wild.

CSV Cleaner solves this problem by allowing users to:

* Define a JSON schema to which the data in a CSV must conform
* Process the CSV data to replace any illegal values with user-defined defaults
* Produce a JSON output report logging any how many illegal values were found
and replaced in the different columns along with some sample values.

The tool is designed to fit into a data pipeline where it can be used to:

* Ensure that user data is clean before being sent to downstream processing,
e.g. by ensuring that an integer type column can be loaded into a database 
without causing the pipeline to fail due to a type error.
* Keep logs files of how much data is being lost during cleaning along with 
sample values.
* Configure thresholds to raise warnings or stop the pipeline if unnacceptable 
data loss occurs, e.g. a pipeline script could be set up raise a warning if 
any data is lost during the cleaning process and to stop the pipeline entirely 
if more than 5% of data is lost in a particular column. 

## Sample Inputs and Outputs

CSV Cleaner takes as its inputs a CSV file, e.g.

```
INT_COLUMN,STRING_COLUMN,DATE_COLUMN,ENUM_COLUMN
4,dog,2020-12-31,V1
not_an_int,cat,not_a_date,V2
an_int,weasel,a_date,V5
```

and a JSON CSV schema definition of that CSV, e.g.:

```json
{
    "columns": [
        {
            "name": "INT_COLUMN",
            "column_type": "Int"
        },
        {
            "name": "STRING_COLUMN",
            "column_type": "String",
            "nullable": false
        },
        {
            "name": "DATE_COLUMN",
            "column_type": "Date",
            "format": "%Y-%m-%d"
        },
        {
            "name": "ENUM_COLUMN",
            "column_type": "Enum",
            "nullable": false,
            "legal_vals": ["V1", "V2", "V3"],
            "illegal_val_replacement": "V1"
        }
    ]
}
```

It outputs a cleaned CSV file, e.g.:

```
INT_COLUMN|STRING_COLUMN|DATE_COLUMN|ENUM_COLUMN|
4,dog,2020-12-31,V1
,cat,,V2
,weasel,,V1
```

and a JSON log indicating which data has been lost during the clean

```json
{
	"total_rows": 3,
	"columns_with_errors": [
		{
			"column_name": "DATE_COLUMN",
			"invalid_row_count": 2,
			"max_illegal_val": "not_a_date",
			"min_illegal_val": "a_date"
		},{
			"column_name": "INT_COLUMN",
			"invalid_row_count": 2,
			"max_illegal_val": "not_an_int",
			"min_illegal_val": "an_int"
		},{
			"column_name": "STRING_COLUMN",
			"invalid_row_count": 0,
			"max_illegal_val": "",
			"min_illegal_val": ""
		},{
			"column_name": "ENUM_COLUMN",
			"invalid_row_count": 1,
			"max_illegal_val": "V5",
			"min_illegal_val": "V5"
		}
	]
}
```