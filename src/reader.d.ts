export interface ColumnSchema {
    name: string;
    type: 'string' | 'int32' | 'int64' | 'float32' | 'float64' | 'boolean' | 'timestamp';
}

export interface ReadResult {
    schema: ColumnSchema[];
    data: Record<string, any[]>;
    numRows: number;
}

/**
 * Read a Parquet file and return columnar data.
 *
 * @param fileBytes - Raw Parquet file bytes.
 * @param maxRows - Maximum rows to decode. Default: 500.
 */
export function readParquet(fileBytes: Uint8Array, maxRows?: number): Promise<ReadResult>;
