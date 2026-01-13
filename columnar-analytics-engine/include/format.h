// Columnar Analytics Engine
// Author: RIAL Fares
// File format specification and I/O

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <fstream>

namespace columnar {

// All multi-byte integers are stored in little-endian format
// Strings are UTF-8 encoded

// Column data types
enum class ColumnType : uint8_t {
    INT32 = 0,
    INT64 = 1,
    STRING = 2
};

// Encoding schemes
enum class EncodingType : uint8_t {
    PLAIN = 0,          // Raw values
    RLE = 1,            // Run-Length Encoding
    DELTA = 2,          // Delta encoding for integers
    DICTIONARY = 3      // Dictionary encoding for strings
};

// File format constants
constexpr uint32_t FILE_MAGIC = 0x454C4F43;  // "COLE" in little-endian
constexpr uint32_t FOOTER_MAGIC = 0x464F4F54;  // "FOOT" in little-endian
constexpr uint16_t FORMAT_VERSION_MAJOR = 1;
constexpr uint16_t FORMAT_VERSION_MINOR = 0;

// Statistics for a page (enables predicate pushdown)
struct PageStats {
    std::optional<int64_t> min_int;
    std::optional<int64_t> max_int;
    uint32_t null_count;
    uint32_t distinct_count_estimate;  // Approximate, 0 if unknown
};

// Column schema
struct ColumnSchema {
    std::string name;
    ColumnType type;
    EncodingType encoding;
};

// Schema for the entire file
struct Schema {
    std::vector<ColumnSchema> columns;

    size_t columnIndex(const std::string& name) const;
    bool hasColumn(const std::string& name) const;
};

// Page header (precedes page data)
struct PageHeader {
    uint32_t uncompressed_size;
    uint32_t compressed_size;
    uint32_t num_values;
    EncodingType encoding;
    PageStats stats;
};

// Column chunk metadata (one per column in a row group)
struct ColumnChunkMeta {
    uint64_t file_offset;
    uint64_t total_size;
    std::vector<PageHeader> page_headers;
};

// Row group metadata
struct RowGroupMeta {
    uint32_t num_rows;
    std::vector<ColumnChunkMeta> column_chunks;
};

// File metadata (stored near end of file)
struct FileMetadata {
    Schema schema;
    std::vector<RowGroupMeta> row_groups;
    uint32_t total_rows;
};

// Writer API
class FileWriter {
public:
    FileWriter(const std::string& path, Schema schema);
    ~FileWriter();

    // Write a batch of rows (all columns must have same length)
    void writeInt32Column(size_t col_idx, const std::vector<int32_t>& values);
    void writeInt64Column(size_t col_idx, const std::vector<int64_t>& values);
    void writeStringColumn(size_t col_idx, const std::vector<std::string>& values);

    // Flush current row group
    void flushRowGroup();

    // Finalize file (writes metadata and footer)
    void close();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Reader API
class FileReader {
public:
    explicit FileReader(const std::string& path);
    ~FileReader();

    const Schema& schema() const;
    const FileMetadata& metadata() const;

    // Read a column chunk from a specific row group
    std::vector<int32_t> readInt32Column(size_t row_group_idx, size_t col_idx);
    std::vector<int64_t> readInt64Column(size_t row_group_idx, size_t col_idx);
    std::vector<std::string> readStringColumn(size_t row_group_idx, size_t col_idx);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace columnar
