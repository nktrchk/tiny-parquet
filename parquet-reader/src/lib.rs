use std::io::Cursor;

use js_sys::{Array, Object, Reflect, Uint8Array};
use parquet2::{
    read::{decompress, get_page_iterator, read_metadata},
    page::Page,
    schema::types::{PhysicalType, PrimitiveLogicalType},
};
use wasm_bindgen::prelude::*;

/// Map physical + logical type to a JS-friendly label
fn type_label(phys: PhysicalType, logical: &Option<PrimitiveLogicalType>) -> &'static str {
    match (phys, logical) {
        (PhysicalType::Int64, Some(PrimitiveLogicalType::Timestamp { .. })) => "timestamp",
        (PhysicalType::Int32, _) => "int32",
        (PhysicalType::Int64, _) => "int64",
        (PhysicalType::Float, _) => "float32",
        (PhysicalType::Double, _) => "float64",
        (PhysicalType::Boolean, _) => "boolean",
        (PhysicalType::ByteArray, _) => "string",
        _ => "binary",
    }
}

/// Decode PLAIN-encoded page buffer into a JS Array.
/// Returns number of values pushed.
fn decode_plain(
    buf: &[u8],
    phys: PhysicalType,
    num_vals: usize,
    arr: &Array,
    limit: usize,
) -> usize {
    let n = num_vals.min(limit);
    match phys {
        PhysicalType::Int32 => {
            for i in 0..n {
                let off = i * 4;
                if off + 4 > buf.len() { return i; }
                let v = i32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                arr.push(&JsValue::from_f64(v as f64));
            }
            n
        }
        PhysicalType::Int64 => {
            for i in 0..n {
                let off = i * 8;
                if off + 8 > buf.len() { return i; }
                let v = i64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
                arr.push(&JsValue::from_f64(v as f64));
            }
            n
        }
        PhysicalType::Float => {
            for i in 0..n {
                let off = i * 4;
                if off + 4 > buf.len() { return i; }
                let v = f32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                arr.push(&JsValue::from_f64(v as f64));
            }
            n
        }
        PhysicalType::Double => {
            for i in 0..n {
                let off = i * 8;
                if off + 8 > buf.len() { return i; }
                let v = f64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
                arr.push(&JsValue::from_f64(v));
            }
            n
        }
        PhysicalType::Boolean => {
            for i in 0..n {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                if byte_idx >= buf.len() { return i; }
                arr.push(&JsValue::from_bool((buf[byte_idx] >> bit_idx) & 1 == 1));
            }
            n
        }
        PhysicalType::ByteArray => {
            let mut off = 0;
            let mut count = 0;
            while count < n && off + 4 <= buf.len() {
                let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                if off + len > buf.len() { break; }
                let s = std::str::from_utf8(&buf[off..off + len]).unwrap_or("<binary>");
                arr.push(&JsValue::from_str(s));
                off += len;
                count += 1;
            }
            count
        }
        _ => 0,
    }
}

/// Read a Parquet file from bytes and return { schema, data, numRows }.
///
/// - `data`: raw Uint8Array of the entire Parquet file
/// - `max_rows`: optional row limit (default 500, for preview)
///
/// Returns a JS object:
/// ```js
/// {
///   schema: [{ name: "col1", type: "string" }, ...],
///   data:   { col1: ["a", "b"], col2: [1, 2], ... },
///   numRows: 12345   // total rows in file (preview may be fewer)
/// }
/// ```
#[wasm_bindgen(js_name = "readParquet")]
pub fn read_parquet(data: &Uint8Array, max_rows: Option<u32>) -> Result<JsValue, JsValue> {
    let bytes = data.to_vec();
    let limit = max_rows.unwrap_or(500) as usize;

    // Read metadata (footer)
    let mut cursor = Cursor::new(&bytes[..]);
    let metadata = read_metadata(&mut cursor)
        .map_err(|e| JsValue::from_str(&format!("metadata: {}", e)))?;

    let col_descriptors = metadata.schema_descr.columns();

    // ── Build JS schema array ────────────────────────────────────────────────
    let schema_arr = Array::new();
    for desc in col_descriptors {
        let obj = Object::new();
        let name = &desc.descriptor.primitive_type.field_info.name;
        let phys = desc.descriptor.primitive_type.physical_type;
        let logical = &desc.descriptor.primitive_type.logical_type;
        Reflect::set(&obj, &"name".into(), &JsValue::from_str(name))?;
        Reflect::set(&obj, &"type".into(), &JsValue::from_str(type_label(phys, logical)))?;
        schema_arr.push(&obj);
    }

    // ── Read column data ─────────────────────────────────────────────────────
    let data_obj = Object::new();

    for rg in &metadata.row_groups {
        for (ci, col_chunk) in rg.columns().iter().enumerate() {
            let desc = &col_descriptors[ci];
            let name = &desc.descriptor.primitive_type.field_info.name;
            let phys = desc.descriptor.primitive_type.physical_type;

            // Fresh cursor per column (get_page_iterator takes reader by value)
            let col_cursor = Cursor::new(&bytes[..]);
            let pages = get_page_iterator(col_chunk, col_cursor, None, vec![], usize::MAX)
                .map_err(|e| JsValue::from_str(&format!("pages[{}]: {}", ci, e)))?;

            let arr = Array::new();
            let mut total = 0usize;

            for maybe in pages {
                if total >= limit { break; }
                let cp = maybe.map_err(|e| JsValue::from_str(&format!("page: {}", e)))?;
                let page = decompress(cp, &mut vec![])
                    .map_err(|e| JsValue::from_str(&format!("decomp: {}", e)))?;
                if let Page::Data(dp) = page {
                    let nv = dp.num_values();
                    total += decode_plain(dp.buffer(), phys, nv, &arr, limit - total);
                }
            }

            Reflect::set(&data_obj, &JsValue::from_str(name), &arr)?;
        }
    }

    // ── Build result object ──────────────────────────────────────────────────
    let result = Object::new();
    Reflect::set(&result, &"schema".into(), &schema_arr)?;
    Reflect::set(&result, &"data".into(), &data_obj)?;
    Reflect::set(
        &result,
        &"numRows".into(),
        &JsValue::from_f64(metadata.num_rows as f64),
    )?;

    Ok(result.into())
}
