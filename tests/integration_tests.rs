use csv::Reader;
use csv_log_cleaner::{clean_csv, get_schema_from_json_str};
use std::env::temp_dir;
use std::fs;

#[test]
fn end_to_end() {
    // Arrange
    let schema_path = format!("tests/e2e_data/test_schema.json");
    let schema_string = fs::read_to_string(schema_path).unwrap();
    let schema_map = get_schema_from_json_str(&schema_string).unwrap();
    let n_body_repetitions = 5000;
    let mock_header = "INT_COLUMN,STRING_COLUMN,DATE_COLUMN,ENUM_COLUMN\n";
    let mock_body = "4,dog,2020-12-31,V1
not_an_int,cat,not_a_date,V2
an_int,weasel,a_date,V5\n"
        .repeat(n_body_repetitions);
    let mock_input = mock_header.to_owned() + &mock_body;
    let expected_log = format!(
        r#"{{
	"total_rows": {},
	"columns_with_errors": [
		{{
			"column_name": "INT_COLUMN",
			"invalid_row_count": {},
			"max_illegal_val": "not_an_int",
			"min_illegal_val": "an_int"
		}},{{
			"column_name": "DATE_COLUMN",
			"invalid_row_count": {},
			"max_illegal_val": "not_a_date",
			"min_illegal_val": "a_date"
		}},{{
			"column_name": "STRING_COLUMN",
			"invalid_row_count": 0,
			"max_illegal_val": "",
			"min_illegal_val": ""
		}},{{
			"column_name": "ENUM_COLUMN",
			"invalid_row_count": {},
			"max_illegal_val": "V5",
			"min_illegal_val": "V5"
		}}
	]
}}"#,
        3 * n_body_repetitions,
        2 * n_body_repetitions,
        2 * n_body_repetitions,
        n_body_repetitions
    );
    let mut rdr = Reader::from_reader(mock_input.as_bytes());
    let dir = temp_dir();
    let output_dir = dir.to_str().unwrap();
    let output_csv_path = format!("{output_dir}/test_output.csv");
    let bytes_sep = ',' as u8;
    let wtr = csv::WriterBuilder::new()
        .delimiter(bytes_sep)
        .from_path(output_csv_path.clone())
        .unwrap();
    let expected_output_csv_path = "tests/e2e_data/expected_output.csv".to_string();
    let expected_output_csv =
        fs::read_to_string(expected_output_csv_path).expect("Failed to read expected csv output");
    let log_path = format!("{output_dir}/test_output.json");
    let buffer_size = 2;
    let expected_header = "INT_COLUMN,STRING_COLUMN,DATE_COLUMN,ENUM_COLUMN\n";
    let expected_row_1 = ",weasel,,V1\n";
    let expected_row_2 = "4,dog,2020-12-31,V1\n";
    let expected_row_3 = ",cat,,V2\n";
    let expected_len = expected_header.len()
        + (expected_row_1.len() + expected_row_2.len() + expected_row_3.len()) * n_body_repetitions;

    // Act
    let result = clean_csv(&mut rdr, wtr, schema_map, buffer_size);
    fs::write(log_path.clone(), result.unwrap().json()).expect("Unable to write file");
    let output_csv =
        fs::read_to_string(output_csv_path).expect("Failed to read output CSV from temp dir");
    let output_log = fs::read_to_string(log_path).expect("Failed to read output log from temp dir");

    // Assert
    println!("expected:");
    println!("{}", expected_output_csv);
    println!("actual:");
    println!("{}", output_csv);
    assert!(output_csv.contains(expected_header));
    assert!(output_csv.contains(expected_row_1));
    assert!(output_csv.contains(expected_row_2));
    assert!(output_csv.contains(expected_row_3));
    assert_eq!(expected_len, output_csv.len());
    println!("expected:");
    println!("{}", expected_log);
    println!("actual:");
    println!("{}", output_log);
    assert_eq!(output_log.len(), expected_log.len());
}
