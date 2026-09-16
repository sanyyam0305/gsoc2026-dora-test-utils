//! TestSink — programmatic sink node for capturing and asserting DORA outputs.
//!
//! The [`run_test_sink`] function creates a DORA node via
//! [`DoraNode::init_from_env`], accumulates all incoming [`Event::Input`]
//! events, and compares them against expected data loaded from a file.

use std::path::PathBuf;

use dora_node_api::{DoraNode, Event};
use eyre::Context;
use serde::{Deserialize, Serialize};

/// Configuration for a test sink run.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Path to the expected output file (DORA JSON format).
    pub expected_file: PathBuf,
    /// Path to write the comparison result to.
    pub output_file: PathBuf,
    /// If true, exit with non-zero on mismatch.
    pub fail_on_mismatch: bool,
    /// If true, use exact JSON string comparison instead of Arrow semantic comparison.
    /// Ignored in record mode.
    pub strict: bool,
    /// If true, skip comparison and write raw received data to output_file.
    /// Default: false (existing behavior).
    pub record_mode: bool,
}

/// Result of a sink comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkResult {
    /// Whether the received data matched the expected data.
    pub r#match: bool,
    /// Number of expected data items.
    pub expected_count: usize,
    /// Number of received data items.
    pub received_count: usize,
    /// List of differences found.
    pub differences: Vec<Difference>,
}

/// A single difference between expected and received data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Difference {
    /// Index of the differing value, or None for structural mismatches.
    pub index: Option<usize>,
    /// Human-readable description of the difference.
    pub message: String,
}

/// Run a test sink: receive inputs, compare with expected, write result.
///
/// # Errors
///
/// Returns an error if:
/// - The expected file cannot be read or parsed
/// - `DoraNode::init_from_env()` fails
pub fn run_test_sink(config: SinkConfig) -> eyre::Result<SinkResult> {
    // ── 1. Initialize DORA node ──────────────────────────────────
    let (_node, mut events) =
        DoraNode::init_from_env().context("failed to initialize DORA node")?;

    // ── 2. Accumulate input events ────────────────────────────────
    let mut received: Vec<arrow::array::ArrayRef> = Vec::new();
    while let Some(event) = events.recv() {
        match event {
            Event::Input { data, .. } => {
                // DoraArray owns an ArrayRef (Arc<dyn Array>); take it.
                received.push(data.into_inner());
            }
            Event::Stop(_) | Event::InputClosed { .. } => break,
            _ => {}
        }
    }

    // ── 3. Record mode: serialize raw data to JSON and return ────
    if config.record_mode {
        return write_record_output(&received, &config.output_file);
    }

    // ── 4. Load expected data ────────────────────────────────────
    let expected_json: serde_json::Value = {
        let contents = std::fs::read_to_string(&config.expected_file).with_context(|| {
            format!(
                "failed to read expected file '{}'",
                config.expected_file.display()
            )
        })?;
        serde_json::from_str(&contents).with_context(|| {
            format!(
                "invalid JSON in expected file '{}'",
                config.expected_file.display()
            )
        })?
    };

    let expected_data = expected_json
        .get("data")
        .ok_or_else(|| eyre::eyre!("expected file missing 'data' field"))?;

    let expected_elements: Vec<&serde_json::Value> = if let Some(arr) = expected_data.as_array() {
        arr.iter().collect()
    } else {
        vec![expected_data]
    };

    // ── 4b. Parse expected data_type for semantic comparison ────
    let expected_data_type: Option<arrow::datatypes::DataType> = expected_json
        .get("data_type")
        .map(|dt| {
            serde_json::from_value(dt.clone())
                .with_context(|| format!("invalid expected data_type: {dt}"))
        })
        .transpose()?;

    // ── 5. Compare ────────────────────────────────────────────────
    let result = if config.strict {
        compare_strict(&expected_elements, &received)?
    } else {
        compare_semantic(&expected_elements, &received, expected_data_type.as_ref())
    };

    // ── 6. Write result ──────────────────────────────────────────
    let result_json = serde_json::to_string_pretty(&result)?;
    std::fs::write(&config.output_file, result_json).with_context(|| {
        format!(
            "failed to write result to '{}'",
            config.output_file.display()
        )
    })?;

    if config.fail_on_mismatch && !result.r#match {
        eyre::bail!(
            "{} difference(s) found: expected {} item(s), received {}",
            result.differences.len(),
            result.expected_count,
            result.received_count
        );
    }

    Ok(result)
}

/// Shared comparison loop: walk two sequences element-by-element,
/// delegating the actual comparison to a closure.  The closure receives
/// `(Option<&E>, Option<&R>, index)` and returns `Some(Difference)`
/// when the elements don't match.
fn compare_sequences<E, R>(
    expected: &[E],
    received: &[R],
    mut compare_element: impl FnMut(Option<&E>, Option<&R>, usize) -> Option<Difference>,
) -> SinkResult {
    let mut differences = Vec::new();

    if expected.len() != received.len() {
        differences.push(Difference {
            index: None,
            message: format!(
                "count mismatch: expected {} but got {}",
                expected.len(),
                received.len()
            ),
        });
    }

    let max_len = expected.len().max(received.len());
    for i in 0..max_len {
        let exp = expected.get(i);
        let rec = received.get(i);
        if let Some(diff) = compare_element(exp, rec, i) {
            differences.push(diff);
        }
    }

    SinkResult {
        r#match: differences.is_empty(),
        expected_count: expected.len(),
        received_count: received.len(),
        differences,
    }
}

/// Strict comparison: serialize received Arrow data back to JSON,
/// compare with serde_json::Value equality.
///
/// # Errors
///
/// Returns an error if any received Arrow array cannot be serialized
/// to JSON (e.g. unsupported types like `List`, `Struct`, `Union`).
fn compare_strict(
    expected: &[&serde_json::Value],
    received: &[arrow::array::ArrayRef],
) -> eyre::Result<SinkResult> {
    use arrow::array::RecordBatch;
    use arrow::datatypes::{Field, Schema};
    use arrow_json::writer::{JsonArray, Writer};
    use std::sync::Arc;

    // Serialize each received Arrow array to JSON.
    let received_json: Vec<serde_json::Value> = received
        .iter()
        .map(|array| {
            let schema = Schema::new(vec![Field::new("data", array.data_type().clone(), true)]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![array.clone()])
                .context("failed to create RecordBatch from received Arrow array")?;

            let mut buf = Vec::new();
            let mut writer = Writer::<_, JsonArray>::new(&mut buf);
            writer
                .write(&batch)
                .context("failed to serialize Arrow array to JSON (type may be unsupported by JsonArray writer)")?;
            writer.finish().context("failed to finish JSON writer")?;

            serde_json::from_slice(&buf)
                .context("invalid JSON produced by arrow_json writer")
        })
        .collect::<eyre::Result<Vec<_>>>()?;

    // Flatten JSON array output into individual elements for comparison.
    let received_flat: Vec<&serde_json::Value> = received_json
        .iter()
        .flat_map(|v| {
            if let Some(arr) = v.as_array() {
                arr.iter().collect::<Vec<_>>()
            } else {
                vec![v]
            }
        })
        .map(|v| {
            if let Some(obj) = v.as_object() {
                obj.get("data").unwrap_or(v)
            } else {
                v
            }
        })
        .collect();

    Ok(compare_sequences(
        expected,
        &received_flat,
        |exp, rec, i| match (exp, rec) {
            (Some(e), Some(r)) if e == r => None,
            (Some(e), Some(r)) => Some(Difference {
                index: Some(i),
                message: format!("value mismatch at index {i}: expected {e}, got {r}"),
            }),
            (Some(_), None) => Some(Difference {
                index: Some(i),
                message: format!("missing received value at index {i}"),
            }),
            (None, Some(_)) => Some(Difference {
                index: Some(i),
                message: format!("unexpected extra value at index {i}"),
            }),
            (None, None) => unreachable!("max_len guarantees at least one is Some"),
        },
    ))
}

/// Semantic comparison: convert expected JSON into Arrow arrays
/// (using the same conversion logic as [`TestSource`]),
/// then compare with the received Arrow data element-by-element.
///
/// Unlike [`compare_strict`], this tolerates **compatible type
/// differences** — e.g. `Int32Array([1, 2])` is considered equal to
/// `Int64Array([1, 2])` because the values are the same.  This is
/// achieved by casting the received array to the expected type
/// before comparing (falling back to exact comparison if the cast
/// fails, e.g. due to overflow).
///
/// Respects `expected_data_type` from the expected file so that
/// e.g. `Int32` expected values are converted to `Int32Array` rather
/// than the default `Int64Array`.
pub fn compare_semantic(
    expected: &[&serde_json::Value],
    received: &[arrow::array::ArrayRef],
    expected_data_type: Option<&arrow::datatypes::DataType>,
) -> SinkResult {
    use std::sync::Arc;

    // Convert expected JSON values to Arrow arrays, including objects/arrays.
    // Record conversion errors as Difference entries for diagnostic clarity.
    let mut differences_from_conversion = Vec::new();
    let expected_arrays: Vec<arrow::array::ArrayRef> = expected
        .iter()
        .enumerate()
        .map(|(i, v)| {
            match crate::source::json_value_to_arrow_array(v, expected_data_type) {
                Ok(arr) => arr,
                Err(e) => {
                    differences_from_conversion.push(Difference {
                        index: Some(i),
                        message: format!(
                            "conversion error at index {i}: {e:#}. Original value: {v}"
                        ),
                    });
                    // Use 0-length NullArray placeholder so the indices stay
                    // aligned.  The comparison closure below skips these
                    // placeholders to avoid redundant "value mismatch"
                    // entries alongside the conversion error above.
                    Arc::new(arrow::array::NullArray::new(0))
                }
            }
        })
        .collect();

    let mut result =
        compare_sequences(&expected_arrays, received, |exp, rec, i| match (exp, rec) {
            (Some(e), Some(r)) => {
                // Skip NullArray(0) placeholders inserted for conversion
                // errors — those are already reported above.
                if e.data_type() == &arrow::datatypes::DataType::Null && e.is_empty() {
                    return None;
                }
                // Semantic equality: when types differ, widen before
                // comparing.  Float types preserve fractional parts,
                // so prefer them over integers to avoid silent
                // truncation (e.g. Float64(1.7) ≠ Int32(1)).
                //
                // Precision guard: widening an integer to a float is lossy
                // above 2^53 (Float64) / 2^24 (Float32).  If the cast
                // doesn't round-trip, the comparison is inconclusive —
                // report a mismatch rather than a false match.
                let lossless = |a: &arrow::array::ArrayRef, t: &arrow::datatypes::DataType| {
                    let fwd = arrow::compute::cast(a, t).ok();
                    let back = fwd
                        .as_ref()
                        .and_then(|v| arrow::compute::cast(v, a.data_type()).ok());
                    match back {
                        Some(b) => b.as_ref() == a.as_ref(),
                        None => false,
                    }
                };
                let matches = if e.data_type() == r.data_type() {
                    e == r
                } else {
                    let target = match (e.data_type(), r.data_type()) {
                        (arrow::datatypes::DataType::Float64, _)
                        | (_, arrow::datatypes::DataType::Float64) => {
                            arrow::datatypes::DataType::Float64
                        }
                        (arrow::datatypes::DataType::Float32, _)
                        | (_, arrow::datatypes::DataType::Float32) => {
                            arrow::datatypes::DataType::Float32
                        }
                        _ => e.data_type().clone(),
                    };
                    let cast_e = arrow::compute::cast(e, &target);
                    let cast_r = arrow::compute::cast(r, &target);
                    match (cast_e, cast_r) {
                        (Ok(ce), Ok(cr)) => {
                            lossless(e, &target)
                                && lossless(r, &target)
                                && ce.as_ref() == cr.as_ref()
                        }
                        _ => false,
                    }
                };
                if matches {
                    None
                } else {
                    Some(Difference {
                        index: Some(i),
                        message: format!(
                            "value mismatch at index {i}: \
                             expected {}[{}], got {}[{}]",
                            e.data_type(),
                            e.len(),
                            r.data_type(),
                            r.len(),
                        ),
                    })
                }
            }
            (Some(_), None) => Some(Difference {
                index: Some(i),
                message: format!("missing received value at index {i}"),
            }),
            (None, Some(_)) => Some(Difference {
                index: Some(i),
                message: format!("unexpected extra value at index {i}"),
            }),
            (None, None) => unreachable!("max_len guarantees at least one is Some"),
        });

    // Prepend conversion errors to the differences list so they appear
    // before any comparison mismatches in the output.
    if !differences_from_conversion.is_empty() {
        differences_from_conversion.append(&mut result.differences);
        result.differences = differences_from_conversion;
        result.r#match = result.differences.is_empty();
    }

    result
}

/// Serialize received Arrow arrays to a JSON record file.
///
/// Format: `{"data": [...], "data_type": "...", "count": N}`
fn write_record_output(
    received: &[arrow::array::ArrayRef],
    output_file: &std::path::Path,
) -> eyre::Result<SinkResult> {
    use arrow::array::RecordBatch;
    use arrow::datatypes::{Field, Schema};
    use arrow_json::writer::{JsonArray, Writer};
    use std::sync::Arc;

    // Serialize each received Arrow array to JSON values.
    let mut all_rows: Vec<serde_json::Value> = Vec::new();
    let mut data_type_json: Option<serde_json::Value> = None;

    for array in received {
        if data_type_json.is_none() {
            // Serialize via serde so the output is parseable by
            // serde_json::from_value::<DataType> in run_test_sink.
            // Store the raw serde_json::Value — simple types become JSON
            // strings ("Int32"), complex types become JSON objects
            // ({"Timestamp": ["Microsecond", null]}).  Both round-trip
            // through serde_json::from_value correctly.
            // The old format!("{:?}") approach produced Debug strings
            // that were unparseable for Struct/List/Timestamp/Decimal.
            data_type_json = serde_json::to_value(array.data_type()).ok();
        }

        let schema = Schema::new(vec![Field::new("data", array.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array.clone()])
            .context("failed to create RecordBatch from received Arrow array")?;

        let mut buf = Vec::new();
        let mut writer = Writer::<_, JsonArray>::new(&mut buf);
        writer.write(&batch).context("Arrow -> JSON write failed")?;
        writer.finish().context("Arrow -> JSON finish failed")?;

        let value: serde_json::Value =
            serde_json::from_slice(&buf).context("invalid JSON from Arrow serialization")?;

        // Each row is an object like {"data": 42}; extract the "data" value.
        if let Some(arr) = value.as_array() {
            for row in arr {
                if let Some(data_val) = row.get("data") {
                    all_rows.push(data_val.clone());
                }
            }
        }
    }

    let record_json = serde_json::json!({
        "data": all_rows,
        "data_type": data_type_json.unwrap_or(serde_json::Value::String("Unknown".to_string())),
        "count": all_rows.len(),
    });

    let json_str = serde_json::to_string_pretty(&record_json)?;
    std::fs::write(output_file, json_str).with_context(|| {
        format!(
            "failed to write record output to '{}'",
            output_file.display()
        )
    })?;

    // Return a SinkResult compatible shape — record mode always "matches" itself.
    Ok(SinkResult {
        r#match: true,
        expected_count: all_rows.len(),
        received_count: all_rows.len(),
        differences: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_compare_semantic_exact_match() {
        let v42 = serde_json::json!(42);
        let v99 = serde_json::json!(99);
        let expected: Vec<&serde_json::Value> = vec![&v42, &v99];
        // Create equivalent Arrow arrays
        let received: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(arrow::array::Int64Array::from(vec![42])),
            Arc::new(arrow::array::Int64Array::from(vec![99])),
        ];
        let result = compare_semantic(&expected, &received, None);
        assert!(result.r#match);
        assert_eq!(result.expected_count, 2);
        assert_eq!(result.received_count, 2);
        assert!(result.differences.is_empty());
    }

    #[test]
    fn test_compare_semantic_count_mismatch() {
        let v1 = serde_json::json!(1);
        let v2 = serde_json::json!(2);
        let expected: Vec<&serde_json::Value> = vec![&v1, &v2];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))];
        let result = compare_semantic(&expected, &received, None);
        assert!(!result.r#match);
        assert_eq!(result.expected_count, 2);
        assert_eq!(result.received_count, 1);
        assert!(!result.differences.is_empty());
    }

    #[test]
    fn test_compare_semantic_value_mismatch() {
        let v42 = serde_json::json!(42);
        let expected: Vec<&serde_json::Value> = vec![&v42];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int64Array::from(vec![99]))];
        let result = compare_semantic(&expected, &received, None);
        assert!(!result.r#match);
        assert_eq!(result.differences.len(), 1);
        assert_eq!(result.differences[0].index, Some(0));
    }

    #[test]
    fn test_compare_semantic_empty_input() {
        let v1 = serde_json::json!(1);
        let expected: Vec<&serde_json::Value> = vec![&v1];
        let received: Vec<arrow::array::ArrayRef> = vec![];
        let result = compare_semantic(&expected, &received, None);
        assert!(!result.r#match);
        assert_eq!(result.received_count, 0);
        assert_eq!(result.expected_count, 1);
    }

    #[test]
    fn test_compare_strict_match() {
        let v42 = serde_json::json!(42);
        let expected: Vec<&serde_json::Value> = vec![&v42];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int64Array::from(vec![42]))];
        let result = compare_strict(&expected, &received).unwrap();
        assert!(result.r#match);
    }

    #[test]
    fn test_compare_strict_mismatch() {
        let v42 = serde_json::json!(42);
        let expected: Vec<&serde_json::Value> = vec![&v42];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int64Array::from(vec![99]))];
        let result = compare_strict(&expected, &received).unwrap();
        assert!(!result.r#match);
    }

    #[test]
    fn test_compare_semantic_cross_type_int32_vs_int64() {
        // Expected is Int32, received is Int64 — semantic comparison
        // should match because the numeric values are equal.
        let v1 = serde_json::json!(1);
        let v2 = serde_json::json!(2);
        let expected: Vec<&serde_json::Value> = vec![&v1, &v2];
        let received: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![2])),
        ];
        // Pass Int32 as the expected data type hint.
        let result = compare_semantic(
            &expected,
            &received,
            Some(&arrow::datatypes::DataType::Int32),
        );
        assert!(
            result.r#match,
            "semantic comparison should tolerate Int32 expected vs Int64 received; got {result:#?}"
        );
        assert!(result.differences.is_empty());
    }

    #[test]
    fn test_compare_semantic_cross_type_float32_vs_float64() {
        let v1 = serde_json::json!(1.0);
        let expected: Vec<&serde_json::Value> = vec![&v1];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Float64Array::from(vec![1.0]))];
        let result = compare_semantic(
            &expected,
            &received,
            Some(&arrow::datatypes::DataType::Float32),
        );
        assert!(
            result.r#match,
            "semantic comparison should tolerate Float32 expected vs Float64 received"
        );
    }

    #[test]
    fn test_compare_semantic_int64_vs_float64_precision_guard() {
        // Int64 values above 2^53 cannot be represented in Float64 — the
        // precision guard must report a mismatch instead of a false match
        // after both sides round to the same f64.
        let v1 = serde_json::json!(9007199254740993i64); // 2^53 + 1
        let expected: Vec<&serde_json::Value> = vec![&v1];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Float64Array::from(vec![
                9007199254740994.0, // 2^53 + 2 — distinct value
            ]))];
        let result = compare_semantic(
            &expected,
            &received,
            Some(&arrow::datatypes::DataType::Int64),
        );
        assert!(
            !result.r#match,
            "lossy Int64→Float64 cast must not false-match; got {result:#?}"
        );

        // Small integers must still match across Int64/Float64.
        let v2 = serde_json::json!(42i64);
        let expected: Vec<&serde_json::Value> = vec![&v2];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Float64Array::from(vec![42.0]))];
        let result = compare_semantic(
            &expected,
            &received,
            Some(&arrow::datatypes::DataType::Int64),
        );
        assert!(
            result.r#match,
            "small Int64 vs Float64 must still match; got {result:#?}"
        );
    }

    #[test]
    fn test_compare_semantic_incompatible_types() {
        // Expected is a string but received is an integer — the Arrow type
        // cast (Int64 → Utf8) succeeds mechanically, but the values differ
        // ("42" ≠ "hello"), so semantic comparison must report a mismatch.
        let expected_str = serde_json::json!("hello");
        let expected: Vec<&serde_json::Value> = vec![&expected_str];
        let received: Vec<arrow::array::ArrayRef> =
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                42,
            ]))];
        let result = compare_semantic(&expected, &received, None);
        assert!(
            !result.r#match,
            "semantic comparison of String vs Int64 should report mismatch, got {result:#?}"
        );
        assert_eq!(result.differences.len(), 1, "expected exactly 1 difference");
        assert_eq!(
            result.differences[0].index,
            Some(0),
            "difference should be at index 0"
        );
    }

    #[test]
    fn test_compare_strict_value_mismatch_string_vs_int() {
        // Strict mode compares JSON representations.  A string expected
        // value and an integer received value are never equal in JSON.
        let expected_str = serde_json::json!("hello");
        let expected: Vec<&serde_json::Value> = vec![&expected_str];
        let received: Vec<arrow::array::ArrayRef> =
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                42,
            ]))];
        let result = compare_strict(&expected, &received).unwrap();
        assert!(
            !result.r#match,
            "strict comparison of String vs Int64 should report mismatch, got {result:#?}"
        );
        assert!(!result.differences.is_empty());
    }

    #[test]
    fn test_compare_semantic_large_batch_1000() {
        // 1000-element comparison must finish correctly in under 500 ms.
        use std::time::Instant;

        let values: Vec<serde_json::Value> = (0_i64..1000).map(|i| serde_json::json!(i)).collect();
        let expected: Vec<&serde_json::Value> = values.iter().collect();
        let received: Vec<arrow::array::ArrayRef> = (0_i64..1000)
            .map(|i| {
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![i]))
                    as arrow::array::ArrayRef
            })
            .collect();

        let start = Instant::now();
        let result = compare_semantic(&expected, &received, None);
        let elapsed = start.elapsed();

        assert!(result.r#match, "1000 identical elements should match");
        assert!(result.differences.is_empty(), "expected 0 differences");
        assert_eq!(result.expected_count, 1000);
        assert_eq!(result.received_count, 1000);
        assert!(
            elapsed.as_millis() < 500,
            "1000-element comparison took {}ms, expected < 500ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn test_compare_semantic_large_batch_1000_one_mismatch() {
        // 1000 elements with the last one wrong → exactly 1 difference reported.
        let mut values: Vec<serde_json::Value> =
            (0_i64..1000).map(|i| serde_json::json!(i)).collect();
        // Corrupt the last expected value.
        values[999] = serde_json::json!(9999_i64);

        let expected: Vec<&serde_json::Value> = values.iter().collect();
        let received: Vec<arrow::array::ArrayRef> = (0_i64..1000)
            .map(|i| {
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![i]))
                    as arrow::array::ArrayRef
            })
            .collect();

        let result = compare_semantic(&expected, &received, None);

        assert!(
            !result.r#match,
            "mismatch at index 999 should fail the comparison"
        );
        assert_eq!(result.expected_count, 1000);
        assert_eq!(result.received_count, 1000);
        assert_eq!(
            result.differences.len(),
            1,
            "expected exactly 1 difference, got {}: {result:#?}",
            result.differences.len()
        );
        assert_eq!(
            result.differences[0].index,
            Some(999),
            "difference should be at index 999"
        );
    }

    #[test]
    fn test_record_mode_output_format() {
        use std::io::Read;

        // Serialize known Arrow data via write_record_output.
        let tmp = tempfile::tempdir().expect("tempdir");
        let output_file = tmp.path().join("record.json");

        let array: arrow::array::ArrayRef =
            Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30]));

        let received = vec![array];

        let result = write_record_output(&received, &output_file)
            .expect("write_record_output should succeed");

        assert!(result.r#match);
        assert_eq!(result.expected_count, 3);
        assert_eq!(result.received_count, 3);

        // Verify output file content.
        let mut contents = String::new();
        std::fs::File::open(&output_file)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).expect("valid JSON");

        assert_eq!(parsed["count"], serde_json::json!(3));
        assert!(parsed["data"].is_array());
        assert_eq!(parsed["data"].as_array().unwrap().len(), 3);
        // Values should be 10, 20, 30 (may be nested under "data" key).
    }

    // ── Conversion error + cast failure edge cases ──────────────

    #[test]
    fn test_compare_semantic_null_value_conversion_error() {
        // serde_json::Value::Null cannot be converted to Arrow — should
        // produce a conversion-error Difference, not a panic.
        let null_val = serde_json::Value::Null;
        let expected: Vec<&serde_json::Value> = vec![&null_val];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int64Array::from(vec![42]))];
        let result = compare_semantic(&expected, &received, None);
        assert!(
            !result.r#match,
            "Null vs Int64 should not match, got {result:#?}"
        );
        assert!(!result.differences.is_empty());
        // Should contain a conversion-error Difference mentioning "null"
        assert!(
            result
                .differences
                .iter()
                .any(|d| d.message.to_lowercase().contains("null")),
            "should mention null in differences, got {result:#?}"
        );
    }

    #[test]
    fn test_compare_semantic_int_overflow_cast() {
        // Expected Int32(500), received Int64 — semantic comparison
        // should cast Int64→Int32 (overflow-safe) and handle the result,
        // OR fall back when the cast fails (the exact behavior is
        // implementation-defined; we only verify it doesn't panic).
        let v500 = serde_json::json!(500);
        let expected: Vec<&serde_json::Value> = vec![&v500];
        let received: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int64Array::from(vec![500]))];
        let result = compare_semantic(
            &expected,
            &received,
            Some(&arrow::datatypes::DataType::Int32),
        );
        // 500 fits in both Int32 and Int64, semantic compare should match.
        assert!(
            result.r#match,
            "Int32(500) vs Int64(500) should match semantically"
        );
    }

    #[test]
    fn test_compare_semantic_boolean_values() {
        let v_true = serde_json::json!(true);
        let v_false = serde_json::json!(false);
        let expected: Vec<&serde_json::Value> = vec![&v_true, &v_false];
        let received: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(arrow::array::BooleanArray::from(vec![true])),
            Arc::new(arrow::array::BooleanArray::from(vec![false])),
        ];
        let result = compare_semantic(&expected, &received, None);
        assert!(
            result.r#match,
            "boolean arrays should match, got {result:#?}"
        );
    }
}
