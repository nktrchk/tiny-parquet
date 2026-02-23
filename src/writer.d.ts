export interface ColumnSchema {
    name: string;
    type: 'string' | 'int32' | 'int64' | 'float32' | 'float64' | 'boolean' | 'timestamp';
}

export interface WriteConfig {
    compression?: 'snappy' | 'none';
}

/**
 * Write a Parquet file from columnar data.
 *
 * @param schema - Column definitions with name and type.
 * @param data - Columnar data keyed by column name.
 * @param config - Optional configuration (compression, etc).
 * @returns The Parquet file as a Uint8Array.
 */
export function writeParquet(
    schema: ColumnSchema[],
    data: Record<string, any[]>,
    config?: WriteConfig,
): Promise<Uint8Array>;
