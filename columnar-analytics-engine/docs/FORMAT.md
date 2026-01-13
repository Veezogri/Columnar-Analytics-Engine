# File Format Specification

Author: RIAL Fares

Version: 1.0

## Overview

The columnar file format is designed for efficient analytical queries with predicate pushdown and data skipping capabilities. All data is organized in row groups, each containing column chunks divided into pages.

## Endianness

All multi-byte integers (uint16, uint32, uint64, int32, int64) are stored in **little-endian** format.

## File Layout

```
+-------------------+
| File Header       |  8 bytes
+-------------------+
| Row Group 1       |  Variable size
|   Column Chunk 1  |
|     Page 1        |
|     Page 2        |
|     ...           |
|   Column Chunk 2  |
|     ...           |
+-------------------+
| Row Group 2       |
|   ...             |
+-------------------+
| ...               |
+-------------------+
| File Metadata     |  Variable size
+-------------------+
| Footer            |  12 bytes
+-------------------+
```

## File Header

Offset | Size | Type    | Description
-------|------|---------|-------------
0      | 4    | uint32  | Magic number: 0x454C4F43 ("COLE")
4      | 2    | uint16  | Version major
6      | 2    | uint16  | Version minor

Total: 8 bytes

## Row Group

A row group contains a fixed number of rows. Each column in the row group is stored as a column chunk.

### Column Chunk

A column chunk consists of one or more pages. Each page is preceded by its header.

### Page Structure

```
+-------------------+
| Page Header       |  Variable size (see below)
+-------------------+
| Page Data         |  compressed_size bytes
+-------------------+
```

### Page Header

Field                     | Type    | Size      | Description
--------------------------|---------|-----------|-------------
uncompressed_size         | uint32  | 4         | Size of data before encoding
compressed_size           | uint32  | 4         | Actual size of page data
num_values                | uint32  | 4         | Number of values in page
encoding                  | uint8   | 1         | Encoding type (see below)
has_stats                 | uint8   | 1         | 1 if stats present, 0 otherwise
stats (conditional)       | varies  | varies    | Statistics (see below)

#### Encoding Types

Value | Name       | Description
------|------------|-------------
0     | PLAIN      | Raw values, no encoding
1     | RLE        | Run-Length Encoding
2     | DELTA      | Delta encoding (integers)
3     | DICTIONARY | Dictionary encoding (strings)

#### Statistics (for numeric columns)

Field       | Type    | Size | Description
------------|---------|------|-------------
has_min     | uint8   | 1    | 1 if min is present
min_value   | int64   | 8    | Minimum value (if has_min = 1)
has_max     | uint8   | 1    | 1 if max is present
max_value   | int64   | 8    | Maximum value (if has_max = 1)
null_count  | uint32  | 4    | Number of null values

Total: 22 bytes when stats are present (min and max both present)

## Page Data Encoding

### PLAIN Encoding

#### INT32
Raw array of int32 values (4 bytes each, little-endian).

#### INT64
Raw array of int64 values (8 bytes each, little-endian).

#### STRING
Format:
```
[offset_array: uint32[num_values + 1]][string_data: bytes]
```

The offset array has `num_values + 1` entries. Entry `i` contains the byte offset where string `i` starts in string_data. Entry `num_values` contains the total size of string_data.

Example: ["hello", "world"]
```
Offsets: [0, 5, 10]
Data: "helloworld"
```

### RLE Encoding (Run-Length Encoding)

Format:
```
[num_runs: varint][run1_length: varint][run1_value: T][run2_length: varint][run2_value: T]...
```

Each run consists of:
- Length (varint): number of consecutive identical values
- Value (T): the repeated value

### DELTA Encoding (integers only)

Format:
```
[base_value: T][num_deltas: varint][delta1: varint][delta2: varint]...
```

First value is stored as-is, subsequent values are stored as deltas from the previous value.

### DICTIONARY Encoding (strings)

Format:
```
[dict_size: uint32]
[dict_entry_0_len: uint32][dict_entry_0_data: bytes]
[dict_entry_1_len: uint32][dict_entry_1_data: bytes]
...
[indices: RLE(uint32)]
```

The dictionary contains unique strings. Indices are RLE-encoded references into the dictionary.

## File Metadata

The metadata section is stored near the end of the file, before the footer. It contains the schema and row group metadata.

### Metadata Structure

Field                  | Type      | Description
-----------------------|-----------|-------------
schema_num_columns     | uint32    | Number of columns
column_schemas         | varies    | Array of column schemas
num_row_groups         | uint32    | Number of row groups
row_group_metas        | varies    | Array of row group metadata
total_rows             | uint32    | Total number of rows in file

### Column Schema

Field         | Type      | Size      | Description
--------------|-----------|-----------|-------------
name_len      | uint32    | 4         | Length of column name
name          | bytes     | name_len  | Column name (UTF-8)
type          | uint8     | 1         | Column type (0=INT32, 1=INT64, 2=STRING)
encoding      | uint8     | 1         | Default encoding type

### Row Group Metadata

Field                  | Type      | Description
-----------------------|-----------|-------------
num_rows               | uint32    | Number of rows in this row group
num_columns            | uint32    | Number of columns
column_chunk_metas     | varies    | Array of column chunk metadata

### Column Chunk Metadata

Field          | Type      | Description
---------------|-----------|-------------
file_offset    | uint64    | Absolute offset in file where column chunk starts
total_size     | uint64    | Total size of column chunk (all pages)
num_pages      | uint32    | Number of pages
page_headers   | varies    | Array of page headers (same structure as in-file page header)

## Footer (Safe Terminator)

The footer is always the last 12 bytes of the file.

Offset | Size | Type    | Description
-------|------|---------|-------------
0      | 4    | uint32  | Footer magic: 0x464F4F54 ("FOOT")
4      | 8    | uint64  | Offset to start of file metadata section

Total: 12 bytes

## Reading Algorithm

1. Seek to end of file minus 12 bytes
2. Read footer (verify magic number)
3. Seek to metadata offset from footer
4. Read file metadata (schema, row groups)
5. Use row group offsets to read specific column chunks
6. Use page headers and statistics for predicate pushdown

## Varint Encoding

Variable-length integers (varint) use a compact encoding for small values:
- Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag
- MSB = 1 means more bytes follow
- MSB = 0 means this is the last byte
- Bytes are in little-endian order (least significant byte first)

Example: 300 (0x12C) encodes as: [0xAC, 0x02]
- 0xAC = 10101100 (continuation bit set, data bits = 0101100)
- 0x02 = 00000010 (final byte, data bits = 0000010)
- Reconstructed: 0000010 0101100 = 100101100 = 300

Signed integers use zigzag encoding: `(n << 1) ^ (n >> 31)` for int32, `(n << 1) ^ (n >> 63)` for int64
