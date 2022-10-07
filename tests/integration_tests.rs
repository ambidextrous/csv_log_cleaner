use csv_cleaner::generate_validated_schema;
use csv_cleaner::process_rows;
use csv_cleaner::JsonSchema;
use std::env::temp_dir;
use std::fs;

#[test]
fn end_to_end() {
    // Arrange
    let schema_path = format!("tests/e2e_data/test_schema.json");
    let schema_string =
        fs::read_to_string("tests/e2e_data/test_schema.json").expect("Failed reading JSON schema");
    let json_schema: JsonSchema =
        serde_json::from_str(&schema_string).expect("Failed parsing JSON schema to struct");
    let input_csv_path = "tests/e2e_data/test_input.csv".to_string();
    let dir = temp_dir();
    let output_dir = dir.to_str().unwrap();
    let output_csv_path = format!("{output_dir}/test_output.csv");
    let expected_output_csv_path = "tests/e2e_data/expected_output.csv".to_string();
    let expected_output_csv =
        fs::read_to_string(expected_output_csv_path).expect("Failed to read expected csv output");
    let log_path = format!("{output_dir}/test_output.json");
    let expected_output_log_path = "tests/e2e_data/expected_output.json".to_string();
    let expected_log =
        fs::read_to_string(expected_output_log_path).expect("Failed to read expected log");
    let schema_map =
        generate_validated_schema(json_schema).expect("Failed generating validated schema");
    let char_sep = ',';
    let sep = char_sep as u8;

    // Act
    let result = process_rows(
        &input_csv_path,
        &output_csv_path,
        &log_path,
        &schema_path,
        sep,
    );
    let output_csv =
        fs::read_to_string(output_csv_path).expect("Failed to read output CSV from temp dir");
    let output_log = fs::read_to_string(log_path).expect("Failed to read output log from temp dir");

    // Assert
    println!("expected:");
    println!("{}", expected_output_csv);
    println!("actual:");
    println!("{}", output_csv);
    assert_eq!(output_csv, expected_output_csv);
    println!("expected:");
    println!("{}", expected_log);
    println!("actual:");
    println!("{}", output_log);
    assert_eq!(output_log.len(), expected_log.len());
}
