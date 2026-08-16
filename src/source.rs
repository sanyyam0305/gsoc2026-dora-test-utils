//! TestSource — programmatic source node for injecting test data into DORA dataflows.
//!
//! The [`run_test_source`] function creates a DORA node via
//! [`DoraNode::init_from_env`] and emits pre-loaded data on a
//! configured output.  Designed for both daemon-based dataflows
//! and standalone testing mode (`DORA_TEST_WITH_INPUTS` env var).

use dora_node_api::{DoraNode, MetadataParameters};
use eyre::{Context, Result};

type DataId = dora_node_api::dora_core::config::DataId;

/// Delay before emitting, letting downstream nodes install their input
/// subscriptions (messages sent in the registration window are dropped).
const SUBSCRIBER_SETTLE_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
/// True when running in standalone mode (no daemon): the test-source
/// feeds a DoraNode directly via DORA_TEST_WITH_INPUTS, so there is no
/// daemon to race against and the delivery delays can be skipped.
fn standalone_mode() -> bool {
    std::env::var("DORA_TEST_WITH_INPUTS").is_ok()
}

/// A single output specification: an output ID and its data.
#[derive(Debug, Clone)]
pub struct OutputSpec {
    /// Output identifier to emit data on.
    pub output_id: String,
    /// DORA-format JSON payload: `{"data": [...], "data_type": {...}}`.
    pub data: serde_json::Value,
}

/// Configuration for a test source run.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// One or more outputs to emit.
    pub outputs: Vec<OutputSpec>,
}

// Backward-compatible constructor from old API.
impl SourceConfig {
    pub fn single(output_id: String, data: serde_json::Value) -> Self {
        Self {
            outputs: vec![OutputSpec { output_id, data }],
        }
    }
}

/// Run a test source: create a DORA node and emit loaded data on
/// one or more outputs.  A single DoraNode is created and reused
/// for all output specs, matching daemon expectations of one
/// Register/OutputsDone lifecycle per process.
///
/// # Errors
///
/// Returns an error if any output spec is invalid, or if node
/// initialization or send_output fails.
pub fn run_test_source(config: SourceConfig) -> Result<()> {
    // Validate all specs before touching the daemon (unit-test safe).
    for spec in &config.outputs {
        validate_spec(spec)?;
    }

    let (mut node, mut events) =
        DoraNode::init_from_env().context("failed to initialize DORA node")?;

    // Let downstream subscribers install their input subscriptions
    // before emitting — messages sent in the registration window are
    // dropped (late-subscriber race, observed at dataflow start).
    // Standalone mode (DORA_TEST_WITH_INPUTS) has no daemon, so the
    // race does not exist there.
    if !standalone_mode() {
        std::thread::sleep(SUBSCRIBER_SETTLE_DELAY);
    }

    for spec in &config.outputs {
        emit_output(&mut node, spec)
            .with_context(|| format!("failed to emit output '{}'", spec.output_id))?;
    }

    // Stay alive until the dataflow stops instead of exiting after a
    // fixed linger.  Exiting closes the input stream while messages are
    // still in flight, and the daemon drops them — a 2 s fixed linger
    // still lost 5 of 10 messages on CI (PR #45).  The daemon sends
    // Stop at --stop-after, so the process lives exactly as long as
    // the dataflow runs; in standalone mode there is no daemon to race.
    if !standalone_mode() {
        while let Some(event) = events.recv() {
            match event {
                dora_node_api::Event::Stop(_) | dora_node_api::Event::InputClosed { .. } => break,
                _ => {}
            }
        }
    }

    Ok(())
}

/// Validate an OutputSpec without touching the daemon.  Fails early
/// with a clear error so unit tests can assert on error messages.
fn validate_spec(spec: &OutputSpec) -> Result<()> {
    let data_array = spec
        .data
        .get("data")
        .ok_or_else(|| eyre::eyre!("missing 'data' field in DORA-format input JSON"))?;
    if !data_array.is_array() {
        eyre::bail!("'data' field must be a JSON array, got: {}", data_array);
    }
    if data_array.as_array().map(|a| a.is_empty()).unwrap_or(false) {
        eyre::bail!("'data' array is empty — nothing to emit");
    }
    // Also validate output_id is parseable
    let _: DataId = spec
        .output_id
        .parse()
        .map_err(|e| eyre::eyre!("invalid output_id '{}': {e}", spec.output_id))?;
    Ok(())
}

fn emit_output(node: &mut DoraNode, spec: &OutputSpec) -> Result<()> {
    // Data was already validated by validate_spec() — unwrap is safe here.
    let elements = spec.data["data"].as_array().unwrap();
    if elements.is_empty() {
        return Ok(());
    }

    // ── Parse data_type hint and convert ──────────────────────────
    let data_type: Option<arrow::datatypes::DataType> = spec
        .data
        .get("data_type")
        .map(|dt| {
            serde_json::from_value(dt.clone())
                .with_context(|| format!("invalid data_type in input JSON: {dt}"))
        })
        .transpose()?;

    // ── Convert each JSON element to an Arrow array ───────────────
    let arrays: Vec<_> = elements
        .iter()
        .map(|v| json_value_to_arrow_array(v, data_type.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    let output_id: DataId = spec
        .output_id
        .parse()
        .map_err(|e| eyre::eyre!("invalid output_id '{}': {e}", spec.output_id))?;

    // ── 4. Emit each array through the shared node ─────────────────
    for array in arrays {
        node.send_output(output_id.clone(), MetadataParameters::default(), array)
            .context("send_output failed")?;
    }

    Ok(())
}

/// Convert a single JSON value to an Arrow array.
///
/// Respects the optional `data_type` hint to produce the correct
/// Arrow type (e.g. `Int32` vs `Int64`).  When `data_type` is
/// `None` the function infers the Arrow type from the JSON value:
/// - JSON number (integer) → Int64Array
/// - JSON number (float) → Float64Array
/// - JSON string → StringArray
/// - JSON bool → BooleanArray
/// - JSON array → wraps in a single-column StructArray via arrow_json
pub(crate) fn json_value_to_arrow_array(
    value: &serde_json::Value,
    data_type: Option<&arrow::datatypes::DataType>,
) -> Result<arrow::array::ArrayRef> {
    use std::sync::Arc;

    match value {
        serde_json::Value::Number(n) => number_to_arrow_array(n, data_type),
        serde_json::Value::String(s) => {
            // Respect data_type hint for string widths.
            match data_type {
                Some(arrow::datatypes::DataType::LargeUtf8) => {
                    Ok(Arc::new(arrow::array::LargeStringArray::from(vec![
                        s.as_str()
                    ])))
                }
                _ => Ok(Arc::new(arrow::array::StringArray::from(vec![s.as_str()]))),
            }
        }
        serde_json::Value::Bool(b) => Ok(Arc::new(arrow::array::BooleanArray::from(vec![*b]))),
        serde_json::Value::Array(arr) => json_array_to_arrow_struct(arr, data_type),
        serde_json::Value::Object(_) => json_obj_to_arrow_struct(value, data_type),
        serde_json::Value::Null => {
            eyre::bail!("null values are not supported as standalone output")
        }
    }
}

/// Convert a JSON number into an Arrow numeric array, respecting the
/// optional `data_type` hint for integer/float width.
fn number_to_arrow_array(
    n: &serde_json::Number,
    data_type: Option<&arrow::datatypes::DataType>,
) -> Result<arrow::array::ArrayRef> {
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    #[allow(clippy::cast_possible_truncation)]
    match data_type {
        Some(DataType::Int8) => {
            let v: i8 = n
                .as_i64()
                .and_then(|i| i8::try_from(i).ok())
                .ok_or_else(|| eyre::eyre!("value {n} out of range for Int8"))?;
            Ok(Arc::new(arrow::array::Int8Array::from(vec![v])))
        }
        Some(DataType::Int16) => {
            let v: i16 = n
                .as_i64()
                .and_then(|i| i16::try_from(i).ok())
                .ok_or_else(|| eyre::eyre!("value {n} out of range for Int16"))?;
            Ok(Arc::new(arrow::array::Int16Array::from(vec![v])))
        }
        Some(DataType::Int32) => {
            let v: i32 = n
                .as_i64()
                .and_then(|i| i32::try_from(i).ok())
                .ok_or_else(|| eyre::eyre!("value {n} out of range for Int32"))?;
            Ok(Arc::new(arrow::array::Int32Array::from(vec![v])))
        }
        Some(DataType::Int64) => {
            if let Some(i) = n.as_i64() {
                Ok(Arc::new(arrow::array::Int64Array::from(vec![i])))
            } else {
                eyre::bail!("value {n} is not representable as Int64")
            }
        }
        Some(DataType::UInt8) => {
            let v: u8 = n
                .as_u64()
                .and_then(|u| u8::try_from(u).ok())
                .ok_or_else(|| eyre::eyre!("value {n} out of range for UInt8"))?;
            Ok(Arc::new(arrow::array::UInt8Array::from(vec![v])))
        }
        Some(DataType::UInt16) => {
            let v: u16 = n
                .as_u64()
                .and_then(|u| u16::try_from(u).ok())
                .ok_or_else(|| eyre::eyre!("value {n} out of range for UInt16"))?;
            Ok(Arc::new(arrow::array::UInt16Array::from(vec![v])))
        }
        Some(DataType::UInt32) => {
            let v: u32 = n
                .as_u64()
                .and_then(|u| u32::try_from(u).ok())
                .ok_or_else(|| eyre::eyre!("value {n} out of range for UInt32"))?;
            Ok(Arc::new(arrow::array::UInt32Array::from(vec![v])))
        }
        Some(DataType::UInt64) => {
            if let Some(u) = n.as_u64() {
                Ok(Arc::new(arrow::array::UInt64Array::from(vec![u])))
            } else {
                eyre::bail!("value {n} is not representable as UInt64")
            }
        }
        // Float16 not handled here — requires the `half` crate.
        Some(DataType::Float32) => {
            let v: f32 = n.as_f64().map(|f| f as f32).ok_or_else(|| {
                eyre::eyre!("value {n} out of range or not representable as Float32")
            })?;
            Ok(Arc::new(arrow::array::Float32Array::from(vec![v])))
        }
        Some(DataType::Float64) => {
            if let Some(f) = n.as_f64() {
                Ok(Arc::new(arrow::array::Float64Array::from(vec![f])))
            } else {
                eyre::bail!("value {n} is not representable as Float64")
            }
        }
        // When the caller didn't request a specific type, infer from the
        // JSON number's shape (integer → Int64, large unsigned → UInt64,
        // fractional → Float64).
        None => {
            if let Some(i) = n.as_i64() {
                Ok(Arc::new(arrow::array::Int64Array::from(vec![i])))
            } else if let Some(u) = n.as_u64() {
                // Value > i64::MAX but fits in u64 — preserve exact integer.
                // serde_json's as_f64() returns Some for every number, so
                // Float64-first would silently truncate large unsigned values.
                Ok(Arc::new(arrow::array::UInt64Array::from(vec![u])))
            } else if let Some(f) = n.as_f64() {
                Ok(Arc::new(arrow::array::Float64Array::from(vec![f])))
            } else {
                eyre::bail!("unsupported number value: {n}")
            }
        }
        // The caller explicitly requested a type this function doesn't
        // know how to produce (e.g. Timestamp, Date32, Decimal128).
        // Don't silently fall back to Int64 — report the gap.
        Some(dt) => {
            eyre::bail!(
                "data_type {dt:?} is not supported for number-to-arrow conversion; \
                 supported types: Int8–Int64, UInt8–UInt64, Float32, Float64"
            );
        }
    }
}

/// Convert a JSON object to a single-row Arrow StructArray.
///
/// When `data_type_hint` is provided, the object is wrapped in
/// `{"data": obj}` before parsing so that the schema field name
/// matches the JSON key.
fn json_obj_to_arrow_struct(
    obj: &serde_json::Value,
    data_type_hint: Option<&arrow::datatypes::DataType>,
) -> Result<arrow::array::ArrayRef> {
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    // JSON objects require an explicit data_type hint so that the schema
    // field name ("data") matches the wrapped JSON structure.  Without a
    // hint, auto-inferred schemas produce N columns but only column(0) is
    // returned — silently dropping all other object keys.
    let dt = data_type_hint.ok_or_else(|| {
        eyre::eyre!(
            "JSON objects require an explicit data_type hint (e.g. \"Struct\"). \
             Without one, auto-inferred multi-column schemas would lose all \
             but the first column."
        )
    })?;

    let schema = Arc::new(Schema::new(vec![Field::new("data", dt.clone(), true)]));

    // Wrap the object in {"data": obj} so that the schema's "data" field
    // name matches the JSON structure.  Serialize as a single JSON object
    // (the arrow_json tape-based decoder expects one JSON object or NDJSON,
    // not a JSON array).
    let wrapped = serde_json::json!({"data": obj});
    let json_bytes = serde_json::to_vec(&wrapped)?;

    json_bytes_to_arrow_column(&json_bytes, schema)
}

/// Convert a JSON array to a single-column Arrow StructArray.
///
/// Each element is wrapped in `{"data": <element>}` and serialized
/// directly (without delegating to [`json_obj_to_arrow_struct`]) to
/// avoid a double-wrapping bug.
fn json_array_to_arrow_struct(
    arr: &[serde_json::Value],
    data_type_hint: Option<&arrow::datatypes::DataType>,
) -> Result<arrow::array::ArrayRef> {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // Determine element data type from hint or infer from first element.
    let element_type: DataType = match data_type_hint {
        Some(dt) => dt.clone(),
        None => match arr.first() {
            Some(serde_json::Value::Number(n)) if n.is_f64() => DataType::Float64,
            Some(serde_json::Value::Number(_)) => DataType::Int64,
            Some(serde_json::Value::String(_)) => DataType::Utf8,
            Some(serde_json::Value::Bool(_)) => DataType::Boolean,
            Some(serde_json::Value::Null) => DataType::Null,
            Some(serde_json::Value::Array(_) | serde_json::Value::Object(_)) => {
                eyre::bail!(
                    "array/object elements in a JSON array require an explicit data type hint"
                )
            }
            None => {
                eyre::bail!("empty JSON array cannot be converted to Arrow without a type hint")
            }
        },
    };

    // Validate that all elements are compatible with the inferred type.
    // Heterogeneous arrays (e.g. [42, "hello"]) would otherwise produce a
    // cryptic arrow_json parse error that doesn't point to the real cause.
    if data_type_hint.is_none() {
        for (i, v) in arr.iter().enumerate().skip(1) {
            let ok = match (&element_type, v) {
                (DataType::Float64, serde_json::Value::Number(_)) => true,
                (DataType::Int64, serde_json::Value::Number(n)) => !n.is_f64(),
                (DataType::Utf8, serde_json::Value::String(_)) => true,
                (DataType::Boolean, serde_json::Value::Bool(_)) => true,
                (DataType::Null, serde_json::Value::Null) => true,
                _ => false,
            };
            if !ok {
                eyre::bail!(
                    "type mismatch in JSON array at index {i}: inferred {element_type:?} \
                     from first element, but element {i} is {}",
                    match v {
                        serde_json::Value::Number(_) => "a number of a different kind",
                        serde_json::Value::String(_) => "a string",
                        serde_json::Value::Bool(_) => "a boolean",
                        serde_json::Value::Null => "null",
                        serde_json::Value::Array(_) => "an array",
                        serde_json::Value::Object(_) => "an object",
                    }
                );
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![Field::new("data", element_type, true)]));

    // Wrap each element in {"data": <element>} and serialize as NDJSON
    // (newline-delimited JSON objects).  The arrow_json tape-based decoder
    // expects a single JSON object or NDJSON, *not* a JSON array, so we
    // avoid serde_json::to_vec(&Vec) which would produce [obj, obj, …].
    let mut json_bytes = Vec::new();
    for v in arr {
        let wrapped = serde_json::json!({"data": v});
        if !json_bytes.is_empty() {
            json_bytes.push(b'\n');
        }
        serde_json::to_writer(&mut json_bytes, &wrapped)?;
    }

    json_bytes_to_arrow_column(&json_bytes, schema)
}

/// Shared helper: parse JSON bytes into an Arrow column via arrow_json,
/// concatenating all batches and extracting the first column.
fn json_bytes_to_arrow_column(
    json_bytes: &[u8],
    schema: std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::RecordBatch;
    use arrow_json::ReaderBuilder;
    use std::io::BufReader;

    let reader = BufReader::new(json_bytes);
    let json_reader = ReaderBuilder::new(schema)
        .build(reader)
        .map_err(|e| eyre::eyre!("failed to build arrow_json reader: {e}"))?;

    let mut batches = Vec::new();
    for result in json_reader {
        let batch: RecordBatch = result.map_err(|e| eyre::eyre!("arrow_json read error: {e}"))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        eyre::bail!("arrow_json produced no batches from input");
    }

    // Merge all batches into one and extract the first column
    let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| eyre::eyre!("failed to concat batches: {e}"))?;

    if merged.num_columns() == 0 {
        eyre::bail!("arrow_json produced zero columns");
    }

    Ok(merged.column(0).clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a minimal SourceConfig for testing.
    fn source_config(data: serde_json::Value) -> SourceConfig {
        SourceConfig::single("test_out".to_string(), data)
    }

    #[test]
    fn test_missing_data_field() {
        let config = source_config(serde_json::json!({"not_data": [1, 2]}));
        let result = run_test_source(config);
        assert!(result.is_err());
        assert!(
            format!("{:#}", result.unwrap_err()).contains("missing 'data' field"),
            "error should mention missing .data. field"
        );
    }

    #[test]
    fn test_empty_data_array() {
        let config = source_config(serde_json::json!({"data": []}));
        let result = run_test_source(config);
        assert!(result.is_err());
        assert!(
            format!("{:#}", result.unwrap_err()).contains("empty"),
            "error should mention empty array"
        );
    }

    #[test]
    fn test_data_not_array() {
        let config = source_config(serde_json::json!({"data": 42}));
        let result = run_test_source(config);
        assert!(result.is_err());
        assert!(
            format!("{:#}", result.unwrap_err()).contains("must be a JSON array"),
            "error should mention must be array"
        );
    }

    #[test]
    fn test_json_to_arrow_int64() {
        let arr = json_value_to_arrow_array(&serde_json::json!(42), None).unwrap();
        assert_eq!(arr.len(), 1);
        let int_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("should be Int64Array");
        assert_eq!(int_arr.value(0), 42);
    }

    #[test]
    fn test_json_to_arrow_float64() {
        let arr =
            json_value_to_arrow_array(&serde_json::json!(std::f64::consts::PI), None).unwrap();
        assert_eq!(arr.len(), 1);
        let float_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("should be Float64Array");
        assert!((float_arr.value(0) - std::f64::consts::PI).abs() < 0.001);
    }

    #[test]
    fn test_json_to_arrow_string() {
        let arr = json_value_to_arrow_array(&serde_json::json!("hello"), None).unwrap();
        assert_eq!(arr.len(), 1);
        let str_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should be StringArray");
        assert_eq!(str_arr.value(0), "hello");
    }

    #[test]
    fn test_json_to_arrow_bool() {
        let arr = json_value_to_arrow_array(&serde_json::json!(true), None).unwrap();
        assert_eq!(arr.len(), 1);
        let bool_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("should be BooleanArray");
        assert!(bool_arr.value(0));
    }

    // ── DataType hint tests ──────────────────────────────────────

    #[test]
    fn test_json_to_arrow_int32() {
        let dt = arrow::datatypes::DataType::Int32;
        let arr = json_value_to_arrow_array(&serde_json::json!(42), Some(&dt)).unwrap();
        let int_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("should be Int32Array from Int32 hint");
        assert_eq!(int_arr.value(0), 42);
    }

    #[test]
    fn test_json_to_arrow_int8() {
        let dt = arrow::datatypes::DataType::Int8;
        let arr = json_value_to_arrow_array(&serde_json::json!(100), Some(&dt)).unwrap();
        let int_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Int8Array>()
            .expect("should be Int8Array from Int8 hint");
        assert_eq!(int_arr.value(0), 100);
    }

    #[test]
    fn test_json_to_arrow_uint8() {
        let dt = arrow::datatypes::DataType::UInt8;
        let arr = json_value_to_arrow_array(&serde_json::json!(255), Some(&dt)).unwrap();
        let uint_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .expect("should be UInt8Array from UInt8 hint");
        assert_eq!(uint_arr.value(0), 255);
    }

    #[test]
    fn test_json_to_arrow_uint8_overflow() {
        let dt = arrow::datatypes::DataType::UInt8;
        // 256 is out of range for u8
        let result = json_value_to_arrow_array(&serde_json::json!(256), Some(&dt));
        assert!(result.is_err());
        assert!(
            format!("{:#}", result.unwrap_err()).contains("out of range for UInt8"),
            "error should mention out of range"
        );
    }

    #[test]
    fn test_json_to_arrow_uint8_negative() {
        let dt = arrow::datatypes::DataType::UInt8;
        // Negative numbers cannot be represented as unsigned
        let result = json_value_to_arrow_array(&serde_json::json!(-1), Some(&dt));
        assert!(result.is_err());
    }

    #[test]
    fn test_json_to_arrow_float32() {
        let dt = arrow::datatypes::DataType::Float32;
        let arr =
            json_value_to_arrow_array(&serde_json::json!(std::f32::consts::PI), Some(&dt)).unwrap();
        let float_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .expect("should be Float32Array from Float32 hint");
        assert!((float_arr.value(0) - std::f32::consts::PI).abs() < 0.001);
    }

    #[test]
    fn test_json_to_arrow_large_utf8() {
        let dt = arrow::datatypes::DataType::LargeUtf8;
        let arr = json_value_to_arrow_array(&serde_json::json!("hello"), Some(&dt)).unwrap();
        let str_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .expect("should be LargeStringArray from LargeUtf8 hint");
        assert_eq!(str_arr.value(0), "hello");
    }

    #[test]
    fn test_json_to_arrow_int64_explicit() {
        let dt = arrow::datatypes::DataType::Int64;
        let arr = json_value_to_arrow_array(&serde_json::json!(42), Some(&dt)).unwrap();
        let int_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("should be Int64Array from Int64 hint");
        assert_eq!(int_arr.value(0), 42);
    }

    #[test]
    fn test_json_to_arrow_int16() {
        let dt = arrow::datatypes::DataType::Int16;
        let arr = json_value_to_arrow_array(&serde_json::json!(42), Some(&dt)).unwrap();
        let int_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::Int16Array>()
            .expect("should be Int16Array from Int16 hint");
        assert_eq!(int_arr.value(0), 42);
    }

    #[test]
    fn test_json_to_arrow_null_panics() {
        let result = json_value_to_arrow_array(&serde_json::Value::Null, None);
        assert!(result.is_err());
        assert!(
            format!("{:#}", result.unwrap_err()).contains("null"),
            "error should mention null"
        );
    }

    #[test]
    fn test_json_to_arrow_unsupported_type_hint_errors() {
        // Timestamp is a valid Arrow type but not supported by number_to_arrow_array.
        let dt =
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);
        let result = json_value_to_arrow_array(&serde_json::json!(42), Some(&dt));
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("not supported"),
            "error should mention unsupported type, got: {msg}"
        );
    }

    #[test]
    fn test_json_to_arrow_single_element() {
        // A single-element JSON array goes through the Value::Array →
        // json_array_to_arrow_struct path and should produce exactly 1 row.
        let arr = json_value_to_arrow_array(&serde_json::json!([42]), None).unwrap();
        assert_eq!(
            arr.len(),
            1,
            "single-element JSON array should produce 1 Arrow row"
        );
    }

    #[test]
    fn test_json_to_arrow_uint32_overflow() {
        // 5_000_000_000 > u32::MAX (4_294_967_295), must fail with "out of range".
        let dt = arrow::datatypes::DataType::UInt32;
        let result = json_value_to_arrow_array(&serde_json::json!(5_000_000_000u64), Some(&dt));
        assert!(
            result.is_err(),
            "value exceeding u32::MAX should return an error"
        );
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("out of range"),
            "error should mention 'out of range', got: {msg}"
        );
    }

    // ── DataType hints: previously untested types ──────────────

    #[test]
    fn test_json_to_arrow_uint16() {
        let dt = arrow::datatypes::DataType::UInt16;
        let arr = json_value_to_arrow_array(&serde_json::json!(1000), Some(&dt)).unwrap();
        let uint_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::UInt16Array>()
            .expect("should be UInt16Array");
        assert_eq!(uint_arr.value(0), 1000);
    }

    #[test]
    fn test_json_to_arrow_uint64() {
        let dt = arrow::datatypes::DataType::UInt64;
        // Value within u64 range (above i64::MAX) — requires as_u64() path
        let arr =
            json_value_to_arrow_array(&serde_json::json!(9_223_372_036_854_775_808u64), Some(&dt))
                .unwrap();
        let uint_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("should be UInt64Array");
        assert_eq!(uint_arr.value(0), 9_223_372_036_854_775_808);
    }

    // ── Object/array edge cases ─────────────────────────────────

    #[test]
    fn test_json_obj_to_arrow_struct_requires_hint() {
        // JSON object without a data_type hint should error.
        let obj = serde_json::json!({"x": 1, "y": 2});
        let result = json_value_to_arrow_array(&obj, None);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("explicit data_type hint"),
            "error should mention explicit hint requirement, got: {msg}"
        );
    }

    #[test]
    fn test_heterogeneous_array_rejection() {
        // [42, "hello"] should fail with a clear type-mismatch error.
        let arr = serde_json::json!([42, "hello"]);
        let result = json_value_to_arrow_array(&arr, None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("type mismatch"),
            "error should mention type mismatch, got: {msg}"
        );
    }

    #[test]
    fn test_fractional_number_with_int64_hint() {
        // 42.5 with Int64 hint — not representable.
        let dt = arrow::datatypes::DataType::Int64;
        let result = json_value_to_arrow_array(&serde_json::json!(42.5), Some(&dt));
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("not representable as Int64"),
            "error should mention not representable, got: {msg}"
        );
    }

    #[test]
    fn test_uint16_overflow() {
        let dt = arrow::datatypes::DataType::UInt16;
        // 70000 > u16::MAX (65535)
        let result = json_value_to_arrow_array(&serde_json::json!(70000), Some(&dt));
        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("out of range for UInt16"));
    }
}
