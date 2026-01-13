// Columnar Analytics Engine
// Author: RIAL Fares
// Encoding implementations

#include "encoding.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace columnar {

// Varint codec implementation
size_t VarintCodec::encodeUInt32(uint32_t value, uint8_t* output) {
    size_t pos = 0;
    while (value >= 0x80) {
        output[pos++] = static_cast<uint8_t>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    output[pos++] = static_cast<uint8_t>(value);
    return pos;
}

size_t VarintCodec::encodeInt32(int32_t value, uint8_t* output) {
    uint32_t encoded = (value << 1) ^ (value >> 31);
    return encodeUInt32(encoded, output);
}

size_t VarintCodec::encodeInt64(int64_t value, uint8_t* output) {
    uint64_t encoded = (static_cast<uint64_t>(value) << 1) ^ (value >> 63);
    size_t pos = 0;
    while (encoded >= 0x80) {
        output[pos++] = static_cast<uint8_t>((encoded & 0x7F) | 0x80);
        encoded >>= 7;
    }
    output[pos++] = static_cast<uint8_t>(encoded);
    return pos;
}

// C2 fix: Safe bounded varint decoding with buffer size validation
uint32_t VarintCodec::decodeUInt32Safe(const uint8_t* data, size_t buffer_size, size_t* bytes_read) {
    constexpr size_t MAX_VARINT32_BYTES = 5;  // 32 bits / 7 bits per byte = 5 bytes max
    uint32_t result = 0;
    int shift = 0;
    size_t pos = 0;

    while (pos < buffer_size && shift < 32) {
        uint8_t byte = data[pos++];
        result |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            if (bytes_read) *bytes_read = pos;
            return result;
        }
        shift += 7;
        if (pos >= MAX_VARINT32_BYTES) {
            throw std::runtime_error("Varint overflow: more than 5 bytes for uint32");
        }
    }

    throw std::runtime_error("Truncated varint: unexpected end of buffer");
}

int32_t VarintCodec::decodeInt32Safe(const uint8_t* data, size_t buffer_size, size_t* bytes_read) {
    uint32_t encoded = decodeUInt32Safe(data, buffer_size, bytes_read);
    return static_cast<int32_t>((encoded >> 1) ^ -(encoded & 1));
}

int64_t VarintCodec::decodeInt64Safe(const uint8_t* data, size_t buffer_size, size_t* bytes_read) {
    constexpr size_t MAX_VARINT64_BYTES = 10;  // 64 bits / 7 bits per byte = 10 bytes max
    uint64_t result = 0;
    int shift = 0;
    size_t pos = 0;

    while (pos < buffer_size && shift < 64) {
        uint8_t byte = data[pos++];
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            if (bytes_read) *bytes_read = pos;
            int64_t decoded = static_cast<int64_t>((result >> 1) ^ -(result & 1));
            return decoded;
        }
        shift += 7;
        if (pos >= MAX_VARINT64_BYTES) {
            throw std::runtime_error("Varint overflow: more than 10 bytes for int64");
        }
    }

    throw std::runtime_error("Truncated varint: unexpected end of buffer");
}

// Legacy unbounded versions - DEPRECATED: use Safe versions instead
uint32_t VarintCodec::decodeUInt32(const uint8_t* data, size_t* bytes_read) {
    uint32_t result = 0;
    int shift = 0;
    size_t pos = 0;

    while (true) {
        uint8_t byte = data[pos++];
        result |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    *bytes_read = pos;
    return result;
}

int32_t VarintCodec::decodeInt32(const uint8_t* data, size_t* bytes_read) {
    uint32_t encoded = decodeUInt32(data, bytes_read);
    return static_cast<int32_t>((encoded >> 1) ^ -(encoded & 1));
}

int64_t VarintCodec::decodeInt64(const uint8_t* data, size_t* bytes_read) {
    uint64_t result = 0;
    int shift = 0;
    size_t pos = 0;

    while (true) {
        uint8_t byte = data[pos++];
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    *bytes_read = pos;
    int64_t decoded = static_cast<int64_t>((result >> 1) ^ -(result & 1));
    return decoded;
}

// RLE encoder
std::vector<uint8_t> RLEEncoder::encodeInt32(const std::vector<int32_t>& values) {
    if (values.empty()) return {};

    std::vector<uint8_t> result;
    result.reserve(values.size() * 5); // Estimate

    size_t i = 0;
    std::vector<std::pair<uint32_t, int32_t>> runs;

    while (i < values.size()) {
        int32_t current = values[i];
        uint32_t run_length = 1;
        while (i + run_length < values.size() && values[i + run_length] == current) {
            run_length++;
        }
        runs.push_back({run_length, current});
        i += run_length;
    }

    uint8_t temp[10];
    size_t len = VarintCodec::encodeUInt32(static_cast<uint32_t>(runs.size()), temp);
    result.insert(result.end(), temp, temp + len);

    for (const auto& [run_len, value] : runs) {
        len = VarintCodec::encodeUInt32(run_len, temp);
        result.insert(result.end(), temp, temp + len);

        len = VarintCodec::encodeInt32(value, temp);
        result.insert(result.end(), temp, temp + len);
    }

    return result;
}

std::vector<uint8_t> RLEEncoder::encodeInt64(const std::vector<int64_t>& values) {
    if (values.empty()) return {};

    std::vector<uint8_t> result;
    result.reserve(values.size() * 9);

    size_t i = 0;
    std::vector<std::pair<uint32_t, int64_t>> runs;

    while (i < values.size()) {
        int64_t current = values[i];
        uint32_t run_length = 1;
        while (i + run_length < values.size() && values[i + run_length] == current) {
            run_length++;
        }
        runs.push_back({run_length, current});
        i += run_length;
    }

    uint8_t temp[10];
    size_t len = VarintCodec::encodeUInt32(static_cast<uint32_t>(runs.size()), temp);
    result.insert(result.end(), temp, temp + len);

    for (const auto& [run_len, value] : runs) {
        len = VarintCodec::encodeUInt32(run_len, temp);
        result.insert(result.end(), temp, temp + len);

        len = VarintCodec::encodeInt64(value, temp);
        result.insert(result.end(), temp, temp + len);
    }

    return result;
}

std::vector<int32_t> RLEEncoder::decodeInt32(const uint8_t* data, size_t size, size_t num_values) {
    std::vector<int32_t> result;
    result.reserve(num_values);

    size_t pos = 0;
    size_t bytes_read = 0;

    // C2 fix: Use safe bounded varint decoding
    uint32_t num_runs = VarintCodec::decodeUInt32Safe(data + pos, size - pos, &bytes_read);
    pos += bytes_read;

    for (uint32_t i = 0; i < num_runs; i++) {
        uint32_t run_length = VarintCodec::decodeUInt32Safe(data + pos, size - pos, &bytes_read);
        pos += bytes_read;

        int32_t value = VarintCodec::decodeInt32Safe(data + pos, size - pos, &bytes_read);
        pos += bytes_read;

        for (uint32_t j = 0; j < run_length; j++) {
            result.push_back(value);
        }
    }

    return result;
}

std::vector<int64_t> RLEEncoder::decodeInt64(const uint8_t* data, size_t size, size_t num_values) {
    std::vector<int64_t> result;
    result.reserve(num_values);

    size_t pos = 0;
    size_t bytes_read = 0;

    // C2 fix: Use safe bounded varint decoding
    uint32_t num_runs = VarintCodec::decodeUInt32Safe(data + pos, size - pos, &bytes_read);
    pos += bytes_read;

    for (uint32_t i = 0; i < num_runs; i++) {
        uint32_t run_length = VarintCodec::decodeUInt32Safe(data + pos, size - pos, &bytes_read);
        pos += bytes_read;

        int64_t value = VarintCodec::decodeInt64Safe(data + pos, size - pos, &bytes_read);
        pos += bytes_read;

        for (uint32_t j = 0; j < run_length; j++) {
            result.push_back(value);
        }
    }

    return result;
}

// Delta encoder
std::vector<uint8_t> DeltaEncoder::encodeInt32(const std::vector<int32_t>& values) {
    if (values.empty()) return {};

    std::vector<uint8_t> result;
    result.reserve(values.size() * 5);

    int32_t base = values[0];
    result.insert(result.end(),
                  reinterpret_cast<const uint8_t*>(&base),
                  reinterpret_cast<const uint8_t*>(&base) + sizeof(int32_t));

    uint8_t temp[10];
    size_t len = VarintCodec::encodeUInt32(static_cast<uint32_t>(values.size() - 1), temp);
    result.insert(result.end(), temp, temp + len);

    int32_t prev = base;
    for (size_t i = 1; i < values.size(); i++) {
        int32_t delta = values[i] - prev;
        len = VarintCodec::encodeInt32(delta, temp);
        result.insert(result.end(), temp, temp + len);
        prev = values[i];
    }

    return result;
}

std::vector<uint8_t> DeltaEncoder::encodeInt64(const std::vector<int64_t>& values) {
    if (values.empty()) return {};

    std::vector<uint8_t> result;
    result.reserve(values.size() * 9);

    int64_t base = values[0];
    result.insert(result.end(),
                  reinterpret_cast<const uint8_t*>(&base),
                  reinterpret_cast<const uint8_t*>(&base) + sizeof(int64_t));

    uint8_t temp[10];
    size_t len = VarintCodec::encodeUInt32(static_cast<uint32_t>(values.size() - 1), temp);
    result.insert(result.end(), temp, temp + len);

    int64_t prev = base;
    for (size_t i = 1; i < values.size(); i++) {
        int64_t delta = values[i] - prev;
        len = VarintCodec::encodeInt64(delta, temp);
        result.insert(result.end(), temp, temp + len);
        prev = values[i];
    }

    return result;
}

std::vector<int32_t> DeltaEncoder::decodeInt32(const uint8_t* data, size_t size, size_t num_values) {
    if (num_values == 0) return {};

    std::vector<int32_t> result;
    result.reserve(num_values);

    int32_t base;
    std::memcpy(&base, data, sizeof(int32_t));
    result.push_back(base);

    size_t pos = sizeof(int32_t);
    size_t bytes_read = 0;

    // C2 fix: Use safe bounded varint decoding
    uint32_t num_deltas = VarintCodec::decodeUInt32Safe(data + pos, size - pos, &bytes_read);
    pos += bytes_read;

    int32_t current = base;
    for (uint32_t i = 0; i < num_deltas; i++) {
        int32_t delta = VarintCodec::decodeInt32Safe(data + pos, size - pos, &bytes_read);
        pos += bytes_read;
        current += delta;
        result.push_back(current);
    }

    return result;
}

std::vector<int64_t> DeltaEncoder::decodeInt64(const uint8_t* data, size_t size, size_t num_values) {
    if (num_values == 0) return {};

    std::vector<int64_t> result;
    result.reserve(num_values);

    int64_t base;
    std::memcpy(&base, data, sizeof(int64_t));
    result.push_back(base);

    size_t pos = sizeof(int64_t);
    size_t bytes_read = 0;

    // C2 fix: Use safe bounded varint decoding
    uint32_t num_deltas = VarintCodec::decodeUInt32Safe(data + pos, size - pos, &bytes_read);
    pos += bytes_read;

    int64_t current = base;
    for (uint32_t i = 0; i < num_deltas; i++) {
        int64_t delta = VarintCodec::decodeInt64Safe(data + pos, size - pos, &bytes_read);
        pos += bytes_read;
        current += delta;
        result.push_back(current);
    }

    return result;
}

// Dictionary encoder
std::vector<uint8_t> DictionaryEncoder::encode(const std::vector<std::string>& values) {
    dict_.clear();
    dict_values_.clear();

    std::vector<uint32_t> indices;
    indices.reserve(values.size());

    for (const auto& value : values) {
        auto it = dict_.find(value);
        if (it == dict_.end()) {
            uint32_t idx = static_cast<uint32_t>(dict_values_.size());
            dict_[value] = idx;
            dict_values_.push_back(value);
            indices.push_back(idx);
        } else {
            indices.push_back(it->second);
        }
    }

    std::vector<uint8_t> result;
    uint32_t dict_size = static_cast<uint32_t>(dict_values_.size());
    result.insert(result.end(),
                  reinterpret_cast<const uint8_t*>(&dict_size),
                  reinterpret_cast<const uint8_t*>(&dict_size) + sizeof(uint32_t));

    for (const auto& str : dict_values_) {
        uint32_t len = static_cast<uint32_t>(str.size());
        result.insert(result.end(),
                      reinterpret_cast<const uint8_t*>(&len),
                      reinterpret_cast<const uint8_t*>(&len) + sizeof(uint32_t));
        result.insert(result.end(), str.begin(), str.end());
    }

    auto encoded_indices = RLEEncoder::encodeInt32(
        std::vector<int32_t>(indices.begin(), indices.end())
    );
    result.insert(result.end(), encoded_indices.begin(), encoded_indices.end());

    return result;
}

std::vector<std::string> DictionaryEncoder::decode(const uint8_t* data, size_t size, size_t num_values) {
    size_t pos = 0;

    uint32_t dict_size;
    std::memcpy(&dict_size, data + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    std::vector<std::string> dictionary;
    dictionary.reserve(dict_size);

    for (uint32_t i = 0; i < dict_size; i++) {
        uint32_t len;
        std::memcpy(&len, data + pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);

        std::string str(reinterpret_cast<const char*>(data + pos), len);
        dictionary.push_back(std::move(str));
        pos += len;
    }

    auto indices = RLEEncoder::decodeInt32(data + pos, size - pos, num_values);

    std::vector<std::string> result;
    result.reserve(num_values);

    for (int32_t idx : indices) {
        if (idx < 0 || idx >= static_cast<int32_t>(dictionary.size())) {
            throw std::runtime_error("Invalid dictionary index");
        }
        result.push_back(dictionary[idx]);
    }

    return result;
}

} // namespace columnar
