use std::io::Cursor;

use js_sys::{Array, Reflect};
use parquet2::{
    compression::CompressionOptions,
    encoding::Encoding,
    metadata::{Descriptor, SchemaDescriptor},
    page::{CompressedPage, DataPage, DataPageHeader, DataPageHeaderV1, DictPage, Page},
    schema::{
        types::{
            FieldInfo, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType,
            PrimitiveType, TimeUnit,
        },
        Repetition,
    },
    write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version, WriteOptions},
};
use wasm_bindgen::prelude::*;

/// Supported column types from JS schema
enum ColType {
    Str,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    TimestampMillis,
}

impl ColType {
    fn from_str(s: &str) -> Self {
        match s {
            "int32" => ColType::Int32,
            "int64" => ColType::Int64,
            "float32" | "float" => ColType::Float32,
            "float64" | "double" => ColType::Float64,
            "boolean" | "bool" => ColType::Boolean,
            "timestamp" | "timestamp_millis" => ColType::TimestampMillis,
            _ => ColType::Str,
        }
    }

    fn physical_type(&self) -> PhysicalType {
        match self {
            ColType::Str => PhysicalType::ByteArray,
            ColType::Int32 => PhysicalType::Int32,
            ColType::Int64 | ColType::TimestampMillis => PhysicalType::Int64,
            ColType::Float32 => PhysicalType::Float,
            ColType::Float64 => PhysicalType::Double,
            ColType::Boolean => PhysicalType::Boolean,
        }
    }

    fn converted_type(&self) -> Option<PrimitiveConvertedType> {
        match self {
            ColType::Str => Some(PrimitiveConvertedType::Utf8),
            ColType::TimestampMillis => Some(PrimitiveConvertedType::TimestampMillis),
            _ => None,
        }
    }

    fn logical_type(&self) -> Option<PrimitiveLogicalType> {
        match self {
            ColType::TimestampMillis => Some(PrimitiveLogicalType::Timestamp {
                unit: TimeUnit::Milliseconds,
                is_adjusted_to_utc: true,
            }),
            _ => None,
        }
    }
}

fn plain_header(n: usize) -> DataPageHeader {
    DataPageHeader::V1(DataPageHeaderV1 {
        num_values: n as i32,
        encoding: Encoding::Plain.into(),
        definition_level_encoding: Encoding::Plain.into(),
        repetition_level_encoding: Encoding::Plain.into(),
        statistics: None,
    })
}

fn dict_header(n: usize) -> DataPageHeader {
    DataPageHeader::V1(DataPageHeaderV1 {
        num_values: n as i32,
        encoding: Encoding::RleDictionary.into(),
        definition_level_encoding: Encoding::Plain.into(),
        repetition_level_encoding: Encoding::Plain.into(),
        statistics: None,
    })
}

fn encode_i32(vals: &[i32], d: &Descriptor) -> Page {
    let mut b = Vec::with_capacity(vals.len() * 4);
    for v in vals { b.extend_from_slice(&v.to_le_bytes()); }
    Page::Data(DataPage::new(plain_header(vals.len()), b, d.clone(), Some(vals.len())))
}

fn encode_i64(vals: &[i64], d: &Descriptor) -> Page {
    let mut b = Vec::with_capacity(vals.len() * 8);
    for v in vals { b.extend_from_slice(&v.to_le_bytes()); }
    Page::Data(DataPage::new(plain_header(vals.len()), b, d.clone(), Some(vals.len())))
}

fn encode_f32(vals: &[f32], d: &Descriptor) -> Page {
    let mut b = Vec::with_capacity(vals.len() * 4);
    for v in vals { b.extend_from_slice(&v.to_le_bytes()); }
    Page::Data(DataPage::new(plain_header(vals.len()), b, d.clone(), Some(vals.len())))
}

fn encode_f64(vals: &[f64], d: &Descriptor) -> Page {
    let mut b = Vec::with_capacity(vals.len() * 8);
    for v in vals { b.extend_from_slice(&v.to_le_bytes()); }
    Page::Data(DataPage::new(plain_header(vals.len()), b, d.clone(), Some(vals.len())))
}

fn encode_bool(vals: &[bool], d: &Descriptor) -> Page {
    let mut b = vec![0u8; (vals.len() + 7) / 8];
    for (i, &v) in vals.iter().enumerate() {
        if v { b[i / 8] |= 1 << (i % 8); }
    }
    Page::Data(DataPage::new(plain_header(vals.len()), b, d.clone(), Some(vals.len())))
}

fn encode_binary(vals: &[Vec<u8>], d: &Descriptor) -> Page {
    let total: usize = vals.iter().map(|v| 4 + v.len()).sum();
    let mut b = Vec::with_capacity(total);
    for v in vals {
        b.extend_from_slice(&(v.len() as u32).to_le_bytes());
        b.extend_from_slice(v);
    }
    Page::Data(DataPage::new(plain_header(vals.len()), b, d.clone(), Some(vals.len())))
}

// ── Dictionary encoding helpers ─────────────────────────────────────────────

/// Number of bits needed to represent values 0..n-1
fn num_bits(n: usize) -> u32 {
    if n <= 1 { return 0; }
    (usize::BITS - (n - 1).leading_zeros())
}

/// Encode u32 indices using RLE/bit-packed hybrid encoding (parquet spec)
fn rle_encode_indices(indices: &[u32], bit_width: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    // bit_width byte prefix (required by Parquet for dict pages)
    buf.push(bit_width as u8);

    if bit_width == 0 {
        // All values are the same (single dictionary entry), write RLE run
        // RLE header: (count << 1) | 0
        let count = indices.len() as u64;
        let header = count << 1;
        let mut h = header;
        loop {
            let byte = (h & 0x7F) as u8;
            h >>= 7;
            if h == 0 {
                buf.push(byte);
                break;
            }
            buf.push(byte | 0x80);
        }
        // value = 0 in ceil(bit_width/8) bytes, but bit_width=0 means 0 bytes... 
        // Actually per spec, min bit_width for RLE is 1
        return buf;
    }

    // Use bit-packed encoding (simpler, works well for random indices)
    let num_values = indices.len();
    let num_groups = (num_values + 7) / 8; // groups of 8

    // bit-packed header: (num_groups << 1) | 1
    let header = ((num_groups as u64) << 1) | 1;
    let mut h = header;
    loop {
        let byte = (h & 0x7F) as u8;
        h >>= 7;
        if h == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }

    // bit-pack the values
    let total_bits = num_groups * 8 * bit_width as usize;
    let total_bytes = (total_bits + 7) / 8;
    let start = buf.len();
    buf.resize(start + total_bytes, 0);

    for (i, &idx) in indices.iter().enumerate() {
        let bit_offset = i * bit_width as usize;
        let byte_offset = start + bit_offset / 8;
        let bit_shift = bit_offset % 8;
        
        // Write the value across potentially multiple bytes
        let mut val = (idx as u64) << bit_shift;
        let bytes_needed = ((bit_shift + bit_width as usize) + 7) / 8;
        for b in 0..bytes_needed {
            if byte_offset + b < buf.len() {
                buf[byte_offset + b] |= (val & 0xFF) as u8;
            }
            val >>= 8;
        }
    }

    buf
}

/// Try to dictionary-encode a binary column in a single pass.
/// Uses Vec linear search (cache-friendly for <256 unique values).
/// Returns None and falls back to plain if cardinality is too high.
fn try_encode_dict(vals: &[Vec<u8>], d: &Descriptor) -> Option<Vec<Page>> {
    let n = vals.len();
    if n == 0 { return None; }

    // Max 256 unique values — keeps Vec linear search fast (≤256 comparisons)
    // and produces optimal RLE encoding (≤8 bits per index)
    let max_unique: usize = 256;

    // Single pass: build dictionary + indices simultaneously
    let mut dict_values: Vec<&[u8]> = Vec::with_capacity(64);
    let mut indices: Vec<u32> = Vec::with_capacity(n);

    for v in vals {
        // Linear search in dict (fast for <256 entries, cache-friendly)
        let idx = dict_values.iter().position(|d| *d == v.as_slice());
        match idx {
            Some(i) => indices.push(i as u32),
            None => {
                if dict_values.len() >= max_unique {
                    return None; // too many unique values, fall back to plain
                }
                indices.push(dict_values.len() as u32);
                dict_values.push(v.as_slice());
            }
        }
    }

    let num_dict = dict_values.len();
    let bits = num_bits(num_dict).max(1);

    // Dictionary page: PLAIN-encoded unique values
    let dict_total: usize = dict_values.iter().map(|v| 4 + v.len()).sum();
    let mut dict_buf = Vec::with_capacity(dict_total);
    for v in &dict_values {
        dict_buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
        dict_buf.extend_from_slice(v);
    }
    let dict_page = Page::Dict(DictPage::new(dict_buf, num_dict, false));

    // Data page: RLE/bit-packed encoded indices
    let rle_buf = rle_encode_indices(&indices, bits);
    let data_page = Page::Data(DataPage::new(
        dict_header(n),
        rle_buf,
        d.clone(),
        Some(n),
    ));

    Some(vec![dict_page, data_page])
}

#[wasm_bindgen(js_name = "writeParquet")]
pub fn write_parquet(
    schema_js: &JsValue,
    data_js: &JsValue,
    config_js: &JsValue,
) -> Result<js_sys::Uint8Array, JsValue> {
    let compression = if let Ok(comp) = Reflect::get(config_js, &"compression".into()) {
        match comp.as_string().as_deref() {
            Some("snappy") => CompressionOptions::Snappy,
            Some("none") => CompressionOptions::Uncompressed,
            _ => CompressionOptions::Snappy,
        }
    } else {
        CompressionOptions::Snappy
    };

    // Dictionary config: default true
    // Note: Reflect::get returns Ok(undefined) when key is missing, NOT Err
    let use_dict = if let Ok(d) = Reflect::get(config_js, &"dictionary".into()) {
        if d.is_undefined() {
            true // key not present → default ON
        } else {
            d.is_truthy() // explicit true/false
        }
    } else {
        true
    };

    let schema_arr: &Array = schema_js
        .dyn_ref::<Array>()
        .ok_or_else(|| JsValue::from_str("schema must be an array"))?;

    let num_cols = schema_arr.length() as usize;
    let mut col_names: Vec<String> = Vec::with_capacity(num_cols);
    let mut col_types: Vec<ColType> = Vec::with_capacity(num_cols);
    let mut parquet_fields: Vec<ParquetType> = Vec::with_capacity(num_cols);

    for i in 0..num_cols {
        let col = schema_arr.get(i as u32);
        let name = Reflect::get(&col, &"name".into())
            .ok()
            .and_then(|v| v.as_string())
            .ok_or_else(|| JsValue::from_str("schema element must have string 'name'"))?;

        let type_str = Reflect::get(&col, &"type".into())
            .ok()
            .and_then(|v| v.as_string())
            .unwrap_or_else(|| "string".to_string());

        let ct = ColType::from_str(&type_str);

        let ptype = PrimitiveType {
            field_info: FieldInfo {
                name: name.clone(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: ct.logical_type(),
            converted_type: ct.converted_type(),
            physical_type: ct.physical_type(),
        };

        parquet_fields.push(ParquetType::PrimitiveType(ptype));
        col_names.push(name);
        col_types.push(ct);
    }

    let schema_desc = SchemaDescriptor::new("schema".to_string(), parquet_fields);
    let columns = schema_desc.columns();

    let options = WriteOptions {
        write_statistics: false,
        version: Version::V1,
    };

    // Build compressed column iterators (FileWriter expects CompressedPage)
    let mut col_iters: Vec<
        Result<
            DynStreamingIterator<'static, CompressedPage, parquet2::error::Error>,
            parquet2::error::Error,
        >,
    > = Vec::with_capacity(num_cols);

    for (i, (name, ct)) in col_names.iter().zip(col_types.iter()).enumerate() {
        let arr_val = Reflect::get(data_js, &JsValue::from_str(name))
            .map_err(|_| JsValue::from_str(&format!("missing column '{}'", name)))?;
        let arr: &Array = arr_val
            .dyn_ref::<Array>()
            .ok_or_else(|| JsValue::from_str(&format!("column '{}' must be array", name)))?;
        let len = arr.length() as usize;
        let desc = columns[i].descriptor.clone();

        let pages: Vec<Page> = match ct {
            ColType::Int32 => {
                let v: Vec<i32> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0) as i32).collect();
                vec![encode_i32(&v, &desc)]
            }
            ColType::Int64 | ColType::TimestampMillis => {
                let v: Vec<i64> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0) as i64).collect();
                vec![encode_i64(&v, &desc)]
            }
            ColType::Float32 => {
                let v: Vec<f32> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0) as f32).collect();
                vec![encode_f32(&v, &desc)]
            }
            ColType::Float64 => {
                let v: Vec<f64> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0)).collect();
                vec![encode_f64(&v, &desc)]
            }
            ColType::Boolean => {
                let v: Vec<bool> = (0..len).map(|j| arr.get(j as u32).is_truthy()).collect();
                vec![encode_bool(&v, &desc)]
            }
            ColType::Str => {
                let v: Vec<Vec<u8>> = (0..len)
                    .map(|j| arr.get(j as u32).as_string().unwrap_or_default().into_bytes())
                    .collect();
                
                if use_dict {
                    try_encode_dict(&v, &desc).unwrap_or_else(|| vec![encode_binary(&v, &desc)])
                } else {
                    vec![encode_binary(&v, &desc)]
                }
            }
        };

        let compressed_pages = DynStreamingIterator::new(Compressor::new_from_vec(
            DynIter::new(pages.into_iter().map(Ok)),
            compression,
            vec![],
        ));
        col_iters.push(Ok(compressed_pages));
    }

    let mut writer = FileWriter::new(Cursor::new(Vec::new()), schema_desc, options, None);

    writer
        .write(DynIter::new(col_iters.into_iter()))
        .map_err(|e| JsValue::from_str(&format!("write error: {}", e)))?;

    writer
        .end(None)
        .map_err(|e| JsValue::from_str(&format!("finalize error: {}", e)))?;

    let bytes = writer.into_inner().into_inner();
    let out = js_sys::Uint8Array::new_with_length(bytes.len() as u32);
    out.copy_from(&bytes);
    Ok(out)
}
