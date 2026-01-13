// Columnar Analytics Engine
// Author: RIAL Fares
// Tests for file format read/write

#include "format.h"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <vector>

using namespace columnar;

const std::string TEST_FILE = "test_format.col";

void cleanup() {
    if (std::filesystem::exists(TEST_FILE)) {
        std::filesystem::remove(TEST_FILE);
    }
}

void test_basic_write_read() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"id", ColumnType::INT64, EncodingType::PLAIN},
        {"value", ColumnType::INT32, EncodingType::PLAIN}
    };

    {
        FileWriter writer(TEST_FILE, schema);

        std::vector<int64_t> ids = {1, 2, 3, 4, 5};
        std::vector<int32_t> values = {10, 20, 30, 40, 50};

        writer.writeInt64Column(0, ids);
        writer.writeInt32Column(1, values);
        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        assert(reader.schema().columns.size() == 2);
        assert(reader.metadata().total_rows == 5);

        auto ids = reader.readInt64Column(0, 0);
        auto values = reader.readInt32Column(0, 1);

        assert(ids.size() == 5);
        assert(values.size() == 5);
        assert(ids[0] == 1 && ids[4] == 5);
        assert(values[0] == 10 && values[4] == 50);
    }

    cleanup();
    std::cout << "test_basic_write_read: PASS\n";
}

void test_rle_encoding() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"category", ColumnType::INT32, EncodingType::RLE}
    };

    std::vector<int32_t> categories = {1, 1, 1, 2, 2, 3, 3, 3, 3};

    {
        FileWriter writer(TEST_FILE, schema);
        writer.writeInt32Column(0, categories);
        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        auto decoded = reader.readInt32Column(0, 0);
        assert(decoded == categories);
    }

    cleanup();
    std::cout << "test_rle_encoding: PASS\n";
}

void test_delta_encoding() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"timestamp", ColumnType::INT64, EncodingType::DELTA}
    };

    std::vector<int64_t> timestamps = {1000, 1100, 1200, 1300, 1400};

    {
        FileWriter writer(TEST_FILE, schema);
        writer.writeInt64Column(0, timestamps);
        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        auto decoded = reader.readInt64Column(0, 0);
        assert(decoded == timestamps);
    }

    cleanup();
    std::cout << "test_delta_encoding: PASS\n";
}

void test_dictionary_encoding() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"region", ColumnType::STRING, EncodingType::DICTIONARY}
    };

    std::vector<std::string> regions = {"north", "south", "north", "east", "south", "north"};

    {
        FileWriter writer(TEST_FILE, schema);
        writer.writeStringColumn(0, regions);
        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        auto decoded = reader.readStringColumn(0, 0);
        assert(decoded == regions);
    }

    cleanup();
    std::cout << "test_dictionary_encoding: PASS\n";
}

void test_string_plain_encoding() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"text", ColumnType::STRING, EncodingType::PLAIN}
    };

    std::vector<std::string> texts = {"hello", "world", "test", "data"};

    {
        FileWriter writer(TEST_FILE, schema);
        writer.writeStringColumn(0, texts);
        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        auto decoded = reader.readStringColumn(0, 0);
        assert(decoded == texts);
    }

    cleanup();
    std::cout << "test_string_plain_encoding: PASS\n";
}

void test_multiple_row_groups() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"value", ColumnType::INT32, EncodingType::PLAIN}
    };

    {
        FileWriter writer(TEST_FILE, schema);

        std::vector<int32_t> batch1 = {1, 2, 3};
        writer.writeInt32Column(0, batch1);
        writer.flushRowGroup();

        std::vector<int32_t> batch2 = {4, 5, 6};
        writer.writeInt32Column(0, batch2);
        writer.flushRowGroup();

        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        assert(reader.metadata().row_groups.size() == 2);
        assert(reader.metadata().total_rows == 6);

        auto rg0 = reader.readInt32Column(0, 0);
        auto rg1 = reader.readInt32Column(1, 0);

        assert(rg0.size() == 3);
        assert(rg1.size() == 3);
        assert(rg0[0] == 1 && rg0[2] == 3);
        assert(rg1[0] == 4 && rg1[2] == 6);
    }

    cleanup();
    std::cout << "test_multiple_row_groups: PASS\n";
}

void test_statistics() {
    cleanup();

    Schema schema;
    schema.columns = {
        {"value", ColumnType::INT64, EncodingType::PLAIN}
    };

    std::vector<int64_t> values = {10, 5, 30, 15, 25};

    {
        FileWriter writer(TEST_FILE, schema);
        writer.writeInt64Column(0, values);
        writer.close();
    }

    {
        FileReader reader(TEST_FILE);
        const auto& rg = reader.metadata().row_groups[0];
        const auto& stats = rg.column_chunks[0].page_headers[0].stats;

        assert(stats.min_int.has_value());
        assert(stats.max_int.has_value());
        assert(stats.min_int.value() == 5);
        assert(stats.max_int.value() == 30);
    }

    cleanup();
    std::cout << "test_statistics: PASS\n";
}

int main() {
    std::cout << "Running format tests...\n";

    test_basic_write_read();
    test_rle_encoding();
    test_delta_encoding();
    test_dictionary_encoding();
    test_string_plain_encoding();
    test_multiple_row_groups();
    test_statistics();

    std::cout << "\nAll format tests passed.\n";
    return 0;
}
