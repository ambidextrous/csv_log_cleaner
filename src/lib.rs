//! Clean CSV files to conform to a type schema by streaming them
//! through small memory buffers using multiple threads and
//! logging data loss.
//!
//! # Documentation
//! [Github](https://github.com/ambidextrous/csv_log_cleaner)

use chrono::NaiveDate;
use csv::{Reader, StringRecord, Writer};
use rayon::{ThreadPool, ThreadPoolBuildError};
use rustc_hash::FxHashMap; // Lots of small HashMaps used, so prioritize fast writes and look ups over collision avoidance
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::io::ErrorKind;
use std::iter::Iterator;
use std::marker::Send;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::vec::Vec;

#[derive(Debug, Clone)]
struct Constants {
    null_vals: Vec<String>,
    bool_vals: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
enum ColumnType {
    String,
    Int,
    Date,
    Float,
    Enum,
    Bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    column_type: ColumnType,
    illegal_val_replacement: String,
    legal_vals: Vec<String>,
    format: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Schema {
    columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonColumn {
    name: String,
    column_type: ColumnType,
    illegal_val_replacement: Option<String>,
    legal_vals: Option<Vec<String>>,
    format: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonSchema {
    columns: Vec<JsonColumn>,
}

#[derive(Debug, Clone)]
struct ProcessRowBufferConfig<'a, W>
where
    W: io::Write + Send + Sync,
{
    column_names: &'a StringRecord,
    schema_map: &'a FxHashMap<String, Column>,
    row_buffer: &'a [FxHashMap<String, String>],
    constants: &'a Constants,
    locked_wtr: Arc<Mutex<Writer<W>>>,
    column_string_names: &'a [String],
    tx: Sender<FxHashMap<String, ColumnLog>>,
}

type StringSender = Sender<Result<(), String>>;

type StringReciever = Receiver<Result<(), String>>;

type MapSender = Sender<FxHashMap<String, ColumnLog>>;

type MapReceiver = Receiver<FxHashMap<String, ColumnLog>>;

/// Holds data on the invalid count and max and min invalid string values
/// (calculated by String comparison) found in that column.
///
/// # Examples
///
/// ```
/// use csv_log_cleaner::ColumnLog;
///
/// let date_of_birth_column_log = ColumnLog {
///     name: "DATE_OF_BIRTH".to_string(),
///     invalid_count: 2,
///     max_invalid: Some("2444-89-01".to_string()),
///     min_invalid: Some("2004-31-01".to_string()),
/// };
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnLog {
    pub name: String,
    pub invalid_count: i32,
    pub max_invalid: Option<String>,
    pub min_invalid: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CleansingLog {
    pub total_rows: i32,
    pub log_map: HashMap<String, ColumnLog>,
}

type Record = FxHashMap<String, String>;

#[derive(Debug)]
pub struct CSVCleansingError {
    message: String,
}

impl CSVCleansingError {
    fn new(message: String) -> CSVCleansingError {
        CSVCleansingError { message }
    }
}

impl Error for CSVCleansingError {}

impl std::fmt::Display for CSVCleansingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Clean CSV files to conform to a type schema by streaming them through small memory buffers using multiple threads and logging data loss.
///
/// # Examples
///
/// ```
/// use std::error::Error;
/// use csv::{Reader,Writer};
/// use csv_log_cleaner::{process_rows, ColumnLog, get_schema_from_json_str};
/// use tempfile::tempdir;
/// use std::fs;
///
/// // Arrange
/// let dir = tempdir().expect("To be able to create temporary directory");
/// let input_csv_data = r#"NAME,AGE,DATE_OF_BIRTH
/// Raul,27,2004-01-31
/// Duke,27.8,2004-31-01
/// "#;
/// let input_path = dir.path().join("input.csv");
/// let output_path = dir.path().join("output.csv");
/// fs::write(input_path.clone(), input_csv_data).expect("To be able to write file");
/// let mut csv_rdr = Reader::from_path(input_path).expect("To be able to create reader");   
/// let mut csv_wtr = Writer::from_path(output_path.clone()).expect("To be able to create writer");
/// let schema_path = dir.path().join("schema.json");
/// let schema_path_str = schema_path.to_str().unwrap();
/// let schema_path_string = String::from(schema_path_str);
/// let schema_string = r#"{
/// "columns": [
///     {
///         "name": "NAME",
///         "column_type": "String"
///     },
///     {
///         "name": "AGE",
///         "column_type": "Int"
///     },
///     {
///         "name": "DATE_OF_BIRTH",
///         "column_type": "Date",
///         "format": "%Y-%m-%d"
///     }
/// ]
/// }"#;
/// let schema_map = get_schema_from_json_str(&schema_string).unwrap();
/// let buffer_size = 1;
/// let expected_date_of_birth_column_log = ColumnLog {
///     name: "DATE_OF_BIRTH".to_string(),
///     invalid_count: 1,
///     max_invalid: Some("2004-31-01".to_string()),
///     min_invalid: Some("2004-31-01".to_string()),
/// };
///
///
/// // Act
/// let result = process_rows(&mut csv_rdr, csv_wtr, schema_map, buffer_size);
/// let output_csv = fs::read_to_string(output_path).expect("To be able to read from file");
///
///
/// // Assert
/// assert!(output_csv.contains("Duke,,\n"));
/// assert!(output_csv.contains("Raul,27,2004-01-31\n"));
/// assert!(output_csv.contains("NAME,AGE,DATE_OF_BIRTH\n"));
/// assert_eq!(output_csv.len(), "NAME,AGE,DATE_OF_BIRTH\nRaul,27,2004-01-31\nDuke,,\n".len());
/// assert_eq!(result.expect("Key to be in map").log_map.get("DATE_OF_BIRTH").unwrap(), &expected_date_of_birth_column_log);
/// ```
pub fn process_rows<R: io::Read, W: io::Write + std::marker::Send + std::marker::Sync + 'static>(
    csv_rdr: &mut Reader<R>,
    csv_wtr: Writer<W>,
    schema_map: FxHashMap<String, Column>,
    buffer_size: usize,
) -> Result<CleansingLog, CSVCleansingError> {
    let result = process_rows_internal(csv_rdr, csv_wtr, schema_map, buffer_size);
    match result {
        Ok(cleansing_log) => Ok(cleansing_log),
        Err(err) => Err(CSVCleansingError::new(err.to_string())),
    }
}

pub fn process_rows_internal<
    R: io::Read,
    W: io::Write + std::marker::Send + std::marker::Sync + 'static,
>(
    csv_rdr: &mut Reader<R>,
    mut csv_wtr: Writer<W>,
    schema_map: FxHashMap<String, Column>,
    buffer_size: usize,
) -> Result<CleansingLog, Box<dyn Error>> {
    // Setup multi-threaded processing
    let (tx, rx): (MapSender, MapReceiver) = mpsc::channel();
    let (error_tx, error_rx): (StringSender, StringReciever) = mpsc::channel();
    let mut row_count = 0;
    let constants = generate_constants();
    let column_names = csv_rdr.headers()?.clone();
    check_spec_valid_for_input(&column_names, &schema_map)?;
    csv_wtr.write_record(&column_names)?;
    let locked_wtr = Arc::new(Mutex::new(csv_wtr));
    let column_string_names: Vec<String> = column_names.iter().map(|x| x.to_string()).collect();
    let mut row_buffer = Vec::new();
    let pool = create_thread_pool()?;
    let mut job_counter = 0;
    // Read input to buffer on one thread; process and write output on other threads as buffer fills
    for row in csv_rdr.deserialize() {
        row_count += 1;
        let row_map: Record = row?;
        row_buffer.push(row_map);
        if row_buffer.len() == buffer_size {
            job_counter += 1;
            let cloned_row_buffer = row_buffer.clone();
            let cloned_schema_map = schema_map.clone();
            let cloned_column_names = column_names.clone();
            let cloned_constants = constants.clone();
            let cloned_locked_wtr = Arc::clone(&locked_wtr);
            let cloned_column_string_names = column_string_names.clone();
            let thread_tx = tx.clone();
            let thread_error_tx = error_tx.clone();
            pool.spawn(move || {
                let row_buffer_data = ProcessRowBufferConfig {
                    column_names: &cloned_column_names,
                    schema_map: &cloned_schema_map,
                    row_buffer: &cloned_row_buffer,
                    constants: &cloned_constants,
                    locked_wtr: cloned_locked_wtr,
                    column_string_names: &cloned_column_string_names,
                    tx: thread_tx,
                };
                process_row_buffer_errors(row_buffer_data, thread_error_tx)
                    .expect("Fatal error calling ThreadPool::spawn");
            });
            row_buffer.clear();
        }
        for potential_error in error_rx.try_iter() {
            potential_error?;
        }
    }
    let thread_tx = tx;

    // Process any remaining rows in buffer
    if !row_buffer.is_empty() {
        job_counter += 1;
        let row_buffer_data = ProcessRowBufferConfig {
            column_names: &column_names,
            schema_map: &schema_map,
            row_buffer: &row_buffer,
            constants: &constants,
            locked_wtr,
            column_string_names: &column_string_names,
            tx: thread_tx,
        };
        process_row_buffer(row_buffer_data)?;
    }

    // Combined logs and raise any error messages sent by threads
    let combined_log_map =
        generate_combined_log_map(&column_names, column_string_names, rx, job_counter)?;
    for potential_error in error_rx.try_iter() {
        potential_error?;
    }

    Ok(CleansingLog {
        total_rows: row_count,
        log_map: copy_to_std_hashmap(combined_log_map),
    })
}

fn process_row_buffer<W>(config: ProcessRowBufferConfig<W>) -> Result<(), Box<dyn Error>>
where
    W: io::Write + Send + Sync,
{
    let mut buffer_log_map =
        generate_column_log_map(config.column_names, config.column_string_names);
    let mut cleaned_rows = Vec::new();
    for row_map in config.row_buffer.iter() {
        let cleaned_row = process_row(
            config.column_names,
            config.schema_map,
            row_map.clone(),
            &mut buffer_log_map,
            config.constants,
        )?;
        cleaned_rows.push(cleaned_row);
    }
    let mut wtr = config
        .locked_wtr
        .lock()
        .expect("Fatal error attempting to aquire Writer in function process_row_buffer");
    for cleaned_row in cleaned_rows.iter() {
        wtr.write_record(cleaned_row)?;
    }
    config.tx.send(buffer_log_map)?;

    Ok(())
}

fn process_row<'a>(
    ordered_column_names: &'a StringRecord,
    schema_dict: &'a FxHashMap<String, Column>,
    row_map: FxHashMap<String, String>,
    log_map: &'a mut FxHashMap<String, ColumnLog>,
    constants: &Constants,
) -> Result<StringRecord, Box<dyn Error>> {
    let mut processed_row = Vec::new();
    for column_name in ordered_column_names {
        let column_value = row_map.get(column_name).ok_or_else(|| {
            format!("Key error, could not find column_name `{column_name}` in row map")
        })?;
        let cleaned_value = column_value.clean(constants);
        let column = schema_dict.get(column_name).ok_or_else(|| {
            format!("Key error, could not find column_name `{column_name}` in schema`")
        })?;
        let processed_value = cleaned_value.process(column, constants);
        if processed_value != cleaned_value {
            let column_log = log_map.get(column_name).ok_or_else(|| {
                format!("Key error, could not find column_name `{column_name}` in log_map`")
            })?;
            let invalid_count = column_log.invalid_count + 1;
            let mut max_invalid = column_log.max_invalid.clone();
            let mut min_invalid = column_log.min_invalid.clone();
            match &column_log.max_invalid {
                Some(x) => {
                    if &processed_value > x {
                        max_invalid = Some(cleaned_value.clone());
                    }
                }
                None => {
                    max_invalid = Some(cleaned_value.clone());
                }
            }
            match &column_log.min_invalid {
                Some(x) => {
                    if &processed_value < x {
                        min_invalid = Some(cleaned_value.clone());
                    }
                }
                None => {
                    min_invalid = Some(cleaned_value.clone());
                }
            }
            let column_log_mut = log_map.get_mut(&column_name.to_string()).ok_or_else(|| {
                format!("Key error, could not find column_name `{column_name}` in log_map`")
            })?;
            column_log_mut.invalid_count = invalid_count;
            column_log_mut.min_invalid = min_invalid;
            column_log_mut.max_invalid = max_invalid;
        }
        processed_row.push(processed_value);
    }
    let processed_record = StringRecord::from(processed_row);

    Ok(processed_record)
}

fn process_row_buffer_errors<W>(
    config: ProcessRowBufferConfig<W>,
    error_tx: Sender<Result<(), String>>,
) -> Result<(), Box<dyn Error>>
where
    W: io::Write + Send + Sync,
{
    let buffer_processing_result = process_row_buffer(config);
    if let Err(err) = buffer_processing_result {
        // Can't send Box<dyn Error>> between threads, so convert e
        // to String before sending through channel
        error_tx.send(Err(err.to_string()))?;
    }

    Ok(())
}

fn copy_to_std_hashmap(fast_map: FxHashMap<String, ColumnLog>) -> HashMap<String, ColumnLog> {
    let mut regular_map = HashMap::new();
    for (key, value) in fast_map {
        regular_map.insert(key, value);
    }
    regular_map
}

fn create_thread_pool() -> Result<ThreadPool, ThreadPoolBuildError> {
    let core_count = num_cpus::get();
    let num_threads = if core_count == 1 { 1 } else { core_count - 1 };
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
}

fn check_spec_valid_for_input(
    column_names: &StringRecord,
    schema_map: &FxHashMap<String, Column>,
) -> Result<(), Box<dyn Error>> {
    let spec_and_csv_columns_match = are_equal_spec_and_csv_columns(&column_names, &schema_map);
    if !spec_and_csv_columns_match {
        return Err(Box::new(CSVCleansingError::new(
            "Error: CSV columns and schema columns do not match".to_string(),
        )));
    }

    Ok(())
}

fn are_equal_spec_and_csv_columns(
    csv_columns_record: &StringRecord,
    spec: &FxHashMap<String, Column>,
) -> bool {
    let mut csv_columns: Vec<String> = csv_columns_record
        .iter()
        .map(|field| field.to_string())
        .collect();
    let mut spec_columns: Vec<String> = spec.keys().cloned().collect();
    csv_columns.sort();
    spec_columns.sort();
    csv_columns == spec_columns
}

fn generate_column_log_map(
    column_names: &StringRecord,
    column_string_names: &[String],
) -> FxHashMap<String, ColumnLog> {
    let column_logs: Vec<ColumnLog> = column_names
        .clone()
        .into_iter()
        .map(|x| ColumnLog {
            name: x.to_string(),
            invalid_count: 0,
            max_invalid: None,
            min_invalid: None,
        })
        .collect();
    let mut_log_map: FxHashMap<String, ColumnLog> = column_string_names
        .iter()
        .cloned()
        .zip(column_logs.iter().cloned())
        .collect();
    mut_log_map
}

fn generate_combined_log_map(
    column_names: &StringRecord,
    column_string_names: Vec<String>,
    rx: MapReceiver,
    mut job_counter: i32,
) -> Result<FxHashMap<String, ColumnLog>, Box<dyn Error>> {
    let mut combined_log_map = generate_column_log_map(column_names, &column_string_names);
    for log_map in rx.iter() {
        job_counter -= 1;
        for (column_name, column_log) in log_map {
            let obtained_log = combined_log_map.get(&column_name.clone()).ok_or_else(|| {
                format!("Key error, could not find column_name `{column_name}` in log map")
            })?;
            let updated_log = obtained_log.update(&column_log);
            combined_log_map.insert(column_name.clone(), updated_log);
        }
        if job_counter < 1 {
            break;
        }
    }

    Ok(combined_log_map)
}

impl ColumnLog {
    fn update(&self, other: &ColumnLog) -> ColumnLog {
        assert!(self.name == other.name);
        let new_invalid_count = self.invalid_count + other.invalid_count;
        let new_max = match (self.max_invalid.clone(), other.max_invalid.clone()) {
            (Some(x), Some(y)) => {
                if x > y {
                    Some(x)
                } else {
                    Some(y)
                }
            }
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            _ => None,
        };
        let new_min = match (self.min_invalid.clone(), other.min_invalid.clone()) {
            (Some(x), Some(y)) => {
                if x < y {
                    Some(x)
                } else {
                    Some(y)
                }
            }
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            _ => None,
        };

        ColumnLog {
            name: self.name.clone(),
            invalid_count: new_invalid_count,
            max_invalid: new_max,
            min_invalid: new_min,
        }
    }
}

pub fn get_schema_from_json_str(
    schema_json_string: &str,
) -> Result<FxHashMap<String, Column>, io::Error> {
    let json_schema: JsonSchema = serde_json::from_str(schema_json_string)?;
    generate_validated_schema(json_schema)
}

impl CleansingLog {
    pub fn json(&self) -> String {
        let mut combined_string = format!(
            "{{\n\t\"total_rows\": {},\n\t\"columns_with_errors\": [\n\t\t",
            self.total_rows
        );
        let mut is_first_row = true;
        for (column_name, column_log) in self.log_map.iter() {
            let mut max_val = String::new();
            {
                if let Some(x) = &column_log.max_invalid {
                    max_val = x.clone();
                }
            }
            let mut min_val = String::new();
            {
                if let Some(x) = &column_log.min_invalid {
                    min_val = x.clone();
                }
            }
            let invalid_row_count = column_log.invalid_count;
            let col_string = format!("{{\n\t\t\t\"column_name\": \"{column_name}\",\n\t\t\t\"invalid_row_count\": {invalid_row_count},\n\t\t\t\"max_illegal_val\": \"{max_val}\",\n\t\t\t\"min_illegal_val\": \"{min_val}\"\n\t\t}}");
            if is_first_row {
                combined_string = format!("{combined_string}{col_string}");
            } else {
                combined_string = format!("{combined_string},{col_string}");
            }
            is_first_row = false;
        }
        combined_string = format!("{combined_string}\n\t]\n}}");
        combined_string
    }
}

fn generate_constants() -> Constants {
    let null_vals = vec![
        "#N/A".to_string(),
        "#N/A".to_string(),
        "N/A".to_string(),
        "#NA".to_string(),
        "-1.#IND".to_string(),
        "-1.#QNAN".to_string(),
        "-NaN".to_string(),
        "-nan".to_string(),
        "1.#IND".to_string(),
        "1.#QNAN".to_string(),
        "<NA>".to_string(),
        "N/A".to_string(),
        "NA".to_string(),
        "NULL".to_string(),
        "NaN".to_string(),
        "n/a".to_string(),
        "nan".to_string(),
        "null".to_string(),
    ];
    let bool_vals = vec![
        "true".to_string(),
        "1".to_string(),
        "1.0".to_string(),
        "yes".to_string(),
        "false".to_string(),
        "0.0".to_string(),
        "0".to_string(),
        "no".to_string(),
    ];
    Constants {
        null_vals,
        bool_vals,
    }
}

fn generate_validated_schema(
    json_schema: JsonSchema,
) -> Result<FxHashMap<String, Column>, io::Error> {
    let empty_vec: Vec<String> = Vec::new();
    let empty_string = String::new();
    let mut column_map: FxHashMap<String, Column> = FxHashMap::default();
    for column in json_schema.columns {
        let new_col = Column {
            column_type: column.column_type,
            illegal_val_replacement: column
                .illegal_val_replacement
                .unwrap_or_else(|| empty_string.clone()),
            legal_vals: column.legal_vals.unwrap_or_else(|| empty_vec.clone()),
            format: column.format.unwrap_or_else(|| empty_string.clone()),
        };

        match column.column_type {
            ColumnType::Date => {
                if new_col.format.is_empty() {
                    let custom_error = io::Error::new(
                        ErrorKind::Other,
                        "Missing required `format` string value for Date column",
                    );
                    return Err(custom_error);
                }
            }
            ColumnType::Enum => {
                if new_col.legal_vals.is_empty() {
                    let custom_error = io::Error::new(
                        ErrorKind::Other,
                        "Missing required `legal_vals` string list value for Enum column",
                    );
                    return Err(custom_error);
                }
            }
            _ => {}
        }
        column_map.insert(column.name, new_col);
    }
    Ok(column_map)
}

trait Process {
    fn process(&self, column: &Column, constants: &Constants) -> Self;
}

impl Process for String {
    fn process(&self, column: &Column, constants: &Constants) -> Self {
        match column.column_type {
            ColumnType::String => self.to_string(),
            ColumnType::Int => {
                let cleaned = self.de_pseudofloat();
                if cleaned.casts_to_int() {
                    cleaned
                } else {
                    column.illegal_val_replacement.to_owned()
                }
            }
            ColumnType::Date => {
                let cleaned = self;
                if cleaned.casts_to_date(&column.format) {
                    cleaned.to_string()
                } else {
                    column.illegal_val_replacement.to_owned()
                }
            }
            ColumnType::Float => {
                let cleaned = self;
                if cleaned.casts_to_float() {
                    cleaned.to_string()
                } else {
                    column.illegal_val_replacement.to_owned()
                }
            }
            ColumnType::Enum => {
                let cleaned = self;
                if cleaned.casts_to_enum(&column.legal_vals) {
                    cleaned.to_string()
                } else {
                    column.illegal_val_replacement.to_owned()
                }
            }
            ColumnType::Bool => {
                let cleaned = self;
                if cleaned.casts_to_bool(constants) {
                    cleaned.to_string()
                } else {
                    column.illegal_val_replacement.to_owned()
                }
            }
        }
    }
}

trait Clean {
    fn clean(&self, constants: &Constants) -> Self;
}

impl Clean for String {
    fn clean(&self, constants: &Constants) -> Self {
        if constants.null_vals.contains(self) {
            String::new()
        } else {
            self.to_string()
        }
    }
}

trait CastsToBool {
    fn casts_to_bool(&self, constants: &Constants) -> bool;
}

impl CastsToBool for String {
    fn casts_to_bool(&self, constants: &Constants) -> bool {
        constants.bool_vals.contains(&self.to_lowercase())
    }
}

trait CastsToEnum {
    fn casts_to_enum(&self, legal_values: &[String]) -> bool;
}

impl CastsToEnum for String {
    fn casts_to_enum(&self, legal_values: &[String]) -> bool {
        legal_values.contains(self)
    }
}

trait CastsToDate {
    fn casts_to_date(&self, format: &str) -> bool;
}

impl CastsToDate for String {
    // `format` parameter should be a value of the form defined here: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    fn casts_to_date(&self, format: &str) -> bool {
        NaiveDate::parse_from_str(self, format).is_ok()
    }
}

trait CastsToInt {
    fn casts_to_int(&self) -> bool;
}

impl CastsToInt for String {
    fn casts_to_int(&self) -> bool {
        // Note: Will return false is number is too large or small
        // to be stored as a 64 bit signed int:
        //     min val: -9_223_372_036_854_775_808
        //     max val: 9_223_372_036_854_775_807
        self.parse::<i64>().is_ok()
    }
}

trait CastsToFloat {
    fn casts_to_float(&self) -> bool;
}

impl CastsToFloat for String {
    fn casts_to_float(&self) -> bool {
        // Note: Will return false is number is too large or small
        // to be stored as a 64 bit signed float:
        //     min val: -1.7976931348623157E+308f64
        //     max val: 1.7976931348623157E+308f64
        self.parse::<f64>().is_ok()
    }
}

trait DePseudofloat {
    fn de_pseudofloat(&self) -> Self;
}

impl DePseudofloat for String {
    fn de_pseudofloat(&self) -> Self {
        let is_pseudofloat = self.ends_with(".0");
        if is_pseudofloat {
            rem_last_n_chars(self, 2).to_string()
        } else {
            self.to_owned()
        }
    }
}

fn rem_last_n_chars(value: &str, n: i32) -> &str {
    let mut chars = value.chars();
    for _ in 0..n {
        chars.next_back();
    }
    chars.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_string() {
        // Arrange
        let input = vec!["NULL".to_string(), String::new(), " dog\t".to_string()];
        let expected = vec![String::new(), String::new(), " dog\t".to_string()];
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.clean(&constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn converts_to_int() {
        // Arrange
        let input = vec![
            "1".to_string(),
            "-3".to_string(),
            "264634633426".to_string(),
            "dog".to_string(),
            "0.4".to_string(),
            "1.0".to_string(),
        ];
        let expected = vec![true, true, true, false, false, false];
        // Act
        let result = input.iter().map(|x| x.casts_to_int()).collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn de_psuedofloats() {
        // Arrange
        let input = vec![
            String::new(),
            "-3.0".to_string(),
            "264634633426.0".to_string(),
            "dog".to_string(),
            "0.4".to_string(),
            "1.0".to_string(),
        ];
        let expected = vec![
            String::new(),
            "-3".to_string(),
            "264634633426".to_string(),
            "dog".to_string(),
            "0.4".to_string(),
            "1".to_string(),
        ];
        // Act
        let result = input.iter().map(|x| x.de_pseudofloat()).collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn process_string() {
        // Arrange
        let input = vec![String::new(), " foo\t".to_string(), "bar".to_string()];
        let expected = vec![String::new(), " foo\t".to_string(), "bar".to_string()];
        let legal_vals: Vec<String> = Vec::new();
        let column = Column {
            column_type: ColumnType::String,
            illegal_val_replacement: String::new(),
            legal_vals: legal_vals,
            format: String::new(),
        };
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.process(&column, &constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn process_int() {
        // Arrange
        let input = vec!["1".to_string(), "-2.0".to_string(), "3134.4".to_string()];
        let expected = vec!["1".to_string(), "-2".to_string(), String::new()];
        let legal_vals: Vec<String> = Vec::new();
        let column = Column {
            column_type: ColumnType::Int,
            illegal_val_replacement: String::new(),
            legal_vals: legal_vals,
            format: String::new(),
        };
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.process(&column, &constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn converts_to_date() {
        // Arrange
        let input = vec![
            "2022-01-31".to_string(),
            "1878-02-03".to_string(),
            "2115-04-42".to_string(),
            "dog".to_string(),
            "31-01-2022".to_string(),
        ];
        let expected = vec![true, true, false, false, false];
        // Act
        let result = input
            .iter()
            .map(|x| x.casts_to_date(&"%Y-%m-%d".to_string()))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn process_date() {
        // Arrange
        let input = vec![
            "2020-01-01".to_string(),
            " 2200-12-31\t".to_string(),
            String::new(),
        ];
        let expected = vec!["2020-01-01".to_string(), String::new(), String::new()];
        let legal_vals: Vec<String> = Vec::new();
        let column = Column {
            column_type: ColumnType::Date,
            illegal_val_replacement: String::new(),
            legal_vals: legal_vals,
            format: "%Y-%m-%d".to_string(),
        };
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.process(&column, &constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn converts_to_float() {
        // Arrange
        let input = vec![
            "1.0".to_string(),
            "-3".to_string(),
            "264634633426".to_string(),
            "dog".to_string(),
            "0.4".to_string(),
            String::new(),
        ];
        let expected = vec![true, true, true, false, true, false];
        // Act
        let result = input.iter().map(|x| x.casts_to_float()).collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn process_float() {
        // Arrange
        let input = vec![String::new(), " 0.1\t".to_string(), "123.456".to_string()];
        let expected = vec![String::new(), String::new(), "123.456".to_string()];
        let legal_vals: Vec<String> = Vec::new();
        let column = Column {
            column_type: ColumnType::Float,
            illegal_val_replacement: String::new(),
            legal_vals: legal_vals,
            format: String::new(),
        };
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.process(&column, &constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn converts_to_enum() {
        // Arrange
        let input = vec![
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
            "7".to_string(),
            "0.4".to_string(),
            String::new(),
        ];
        let legal = vec!["A".to_string(), "B".to_string()];
        let expected = vec![true, true, false, false, false, false];
        // Act
        let result = input
            .iter()
            .map(|x| x.casts_to_enum(&legal))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn converts_to_tool() {
        // Arrange
        let input = vec![
            "true".to_string(),
            "false".to_string(),
            "True".to_string(),
            "False".to_string(),
            "0".to_string(),
            "1".to_string(),
            "dog".to_string(),
        ];
        let expected = vec![true, true, true, true, true, true, false];
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.casts_to_bool(&constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn process_enum() {
        // Arrange
        let input = vec![String::new(), " A\t".to_string(), "B".to_string()];
        let expected = vec![String::new(), String::new(), "B".to_string()];
        let legal_vals = vec!["A".to_string(), "B".to_string()];
        let column = Column {
            column_type: ColumnType::Enum,
            illegal_val_replacement: String::new(),
            legal_vals: legal_vals,
            format: String::new(),
        };
        let constants = generate_constants();
        // Act
        let result = input
            .iter()
            .map(|x| x.process(&column, &constants))
            .collect::<Vec<_>>();
        // Assert
        assert_eq!(result, expected);
    }

    #[test]
    fn generate_column() {
        // Arrange
        let raw_schema = r#"
            {
                "columns": [
		    {
			"name": "INT_COLUMN",
			"column_type": "Int",
			"illegal_val_replacement": null,
			"legal_vals": null
		    },
		    {
			"name": "DATE_COLUMN",
			"column_type": "Date",
			"format": "%Y-%m-%d"
		    },
		    {
			"name": "FLOAT_COLUMN",
			"column_type": "Float",
			"illegal_val_replacement": ""
		    },
		    {
			"name": "STRING_COLUMN",
			"column_type": "String"
		    },
		    {
			"name": "BOOL_COLUMN",
			"column_type": "Bool"
		    },
		    {
			"name": "ENUM_COLUMN",
			"column_type": "Enum",
			"illegal_val_replacement": "DEFAULT",
			"legal_vals": ["A", "B", "DEFAULT"]
		    }
                ]
            }"#;
        let json_schema: JsonSchema = serde_json::from_str(raw_schema).unwrap();

        generate_validated_schema(json_schema).unwrap();
    }
}
