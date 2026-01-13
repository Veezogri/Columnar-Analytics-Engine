// Columnar Analytics Engine
// Author: RIAL Fares
// Tests for encoding schemes

#include "encoding.h"
#include <cassert>
#include <iostream>
#include <vector>

using namespace columnar;

void test_varint_encoding() {
    uint8_t buffer[10];

    size_t len = VarintCodec::encodeUInt32(300, buffer);
    assert(len == 2);
    (void)len;

    size_t bytes_read = 0;
    uint32_t decoded = VarintCodec::decodeUInt32(buffer, &bytes_read);
    assert(decoded == 300);
    assert(bytes_read == 2);
    (void)decoded;

    std::cout << "test_varint_encoding: PASS\n";
}

void test_varint_signed() {
    uint8_t buffer[10];

    int32_t values[] = {0, 1, -1, 127, -127, 10000, -10000};

    for (int32_t val : values) {
        size_t len = VarintCodec::encodeInt32(val, buffer);
        size_t bytes_read = 0;
        int32_t decoded = VarintCodec::decodeInt32(buffer, &bytes_read);
        assert(decoded == val);
        assert(bytes_read == len);
        (void)len;
        (void)decoded;
    }

    std::cout << "test_varint_signed: PASS\n";
}

void test_rle_int32() {
    std::vector<int32_t> values = {5, 5, 5, 5, 10, 10, 3, 3, 3, 3, 3};
    auto encoded = RLEEncoder::encodeInt32(values);

    auto decoded = RLEEncoder::decodeInt32(encoded.data(), encoded.size(), values.size());
    assert(decoded == values);

    std::cout << "test_rle_int32: PASS\n";
}

void test_rle_int64() {
    std::vector<int64_t> values = {100, 100, 100, 200, 200, 300};
    auto encoded = RLEEncoder::encodeInt64(values);

    auto decoded = RLEEncoder::decodeInt64(encoded.data(), encoded.size(), values.size());
    assert(decoded == values);

    std::cout << "test_rle_int64: PASS\n";
}

void test_delta_int32() {
    std::vector<int32_t> values = {10, 15, 20, 25, 30};
    auto encoded = DeltaEncoder::encodeInt32(values);

    auto decoded = DeltaEncoder::decodeInt32(encoded.data(), encoded.size(), values.size());
    assert(decoded == values);

    std::cout << "test_delta_int32: PASS\n";
}

void test_delta_int64() {
    std::vector<int64_t> values = {1000, 1005, 1010, 1015, 1020};
    auto encoded = DeltaEncoder::encodeInt64(values);

    auto decoded = DeltaEncoder::decodeInt64(encoded.data(), encoded.size(), values.size());
    assert(decoded == values);

    std::cout << "test_delta_int64: PASS\n";
}

void test_dictionary_encoding() {
    std::vector<std::string> values = {"apple", "banana", "apple", "cherry", "banana", "apple"};

    DictionaryEncoder encoder;
    auto encoded = encoder.encode(values);

    auto decoded = DictionaryEncoder::decode(encoded.data(), encoded.size(), values.size());
    assert(decoded == values);

    std::cout << "test_dictionary_encoding: PASS\n";
}

void test_dictionary_high_cardinality() {
    std::vector<std::string> values;
    for (int i = 0; i < 100; i++) {
        values.push_back("value_" + std::to_string(i % 10));
    }

    DictionaryEncoder encoder;
    auto encoded = encoder.encode(values);

    auto decoded = DictionaryEncoder::decode(encoded.data(), encoded.size(), values.size());
    assert(decoded == values);

    std::cout << "test_dictionary_high_cardinality: PASS\n";
}

int main() {
    std::cout << "Running encoding tests...\n";

    test_varint_encoding();
    test_varint_signed();
    test_rle_int32();
    test_rle_int64();
    test_delta_int32();
    test_delta_int64();
    test_dictionary_encoding();
    test_dictionary_high_cardinality();

    std::cout << "\nAll encoding tests passed.\n";
    return 0;
}
