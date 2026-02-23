use std::io::Cursor;

use js_sys::{Array, Reflect};
use parquet2::{
    compression::CompressionOptions,
    encoding::Encoding,
    metadata::{Descriptor, SchemaDescriptor},
    page::{CompressedPage, DataPage, DataPageHeader, DataPageHeaderV1, Page},
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

        let page = match ct {
            ColType::Int32 => {
                let v: Vec<i32> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0) as i32).collect();
                encode_i32(&v, &desc)
            }
            ColType::Int64 | ColType::TimestampMillis => {
                let v: Vec<i64> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0) as i64).collect();
                encode_i64(&v, &desc)
            }
            ColType::Float32 => {
                let v: Vec<f32> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0) as f32).collect();
                encode_f32(&v, &desc)
            }
            ColType::Float64 => {
                let v: Vec<f64> = (0..len).map(|j| arr.get(j as u32).as_f64().unwrap_or(0.0)).collect();
                encode_f64(&v, &desc)
            }
            ColType::Boolean => {
                let v: Vec<bool> = (0..len).map(|j| arr.get(j as u32).is_truthy()).collect();
                encode_bool(&v, &desc)
            }
            ColType::Str => {
                let v: Vec<Vec<u8>> = (0..len)
                    .map(|j| arr.get(j as u32).as_string().unwrap_or_default().into_bytes())
                    .collect();
                encode_binary(&v, &desc)
            }
        };

        let pages = DynStreamingIterator::new(Compressor::new_from_vec(
            DynIter::new(std::iter::once(Ok(page))),
            compression,
            vec![],
        ));
        col_iters.push(Ok(pages));
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
