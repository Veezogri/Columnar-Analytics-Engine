// Columnar Analytics Engine
// Author: RIAL Fares
// Encoding and compression schemes

#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>

namespace columnar {

// Run-Length Encoding
class RLEEncoder {
public:
    // Encode runs of identical values
    // Format: [run_length: varint][value: T]...
    static std::vector<uint8_t> encodeInt32(const std::vector<int32_t>& values);
    static std::vector<uint8_t> encodeInt64(const std::vector<int64_t>& values);

    static std::vector<int32_t> decodeInt32(const uint8_t* data, size_t size, size_t num_values);
    static std::vector<int64_t> decodeInt64(const uint8_t* data, size_t size, size_t num_values);
};

// Delta Encoding for integers
class DeltaEncoder {
public:
    // Store first value, then deltas
    // Format: [base_value: T][delta1: varint][delta2: varint]...
    static std::vector<uint8_t> encodeInt32(const std::vector<int32_t>& values);
    static std::vector<uint8_t> encodeInt64(const std::vector<int64_t>& values);

    static std::vector<int32_t> decodeInt32(const uint8_t* data, size_t size, size_t num_values);
    static std::vector<int64_t> decodeInt64(const uint8_t* data, size_t size, size_t num_values);
};

// Dictionary Encoding for strings
class DictionaryEncoder {
public:
    DictionaryEncoder() = default;

    // Build dictionary and encode indices
    // Format: [dict_size: uint32][dict_entry_len: uint32][dict_entry: bytes]...[indices: RLE(uint32)]
    std::vector<uint8_t> encode(const std::vector<std::string>& values);

    // Decode from dictionary format
    static std::vector<std::string> decode(const uint8_t* data, size_t size, size_t num_values);

private:
    std::unordered_map<std::string, uint32_t> dict_;
    std::vector<std::string> dict_values_;
};

// Varint encoding utilities (for compact integer storage)
class VarintCodec {
public:
    static size_t encodeUInt32(uint32_t value, uint8_t* output);
    static size_t encodeInt32(int32_t value, uint8_t* output);
    static size_t encodeInt64(int64_t value, uint8_t* output);

    static uint32_t decodeUInt32(const uint8_t* data, size_t* bytes_read);
    static int32_t decodeInt32(const uint8_t* data, size_t* bytes_read);
    static int64_t decodeInt64(const uint8_t* data, size_t* bytes_read);
};

} // namespace columnar
