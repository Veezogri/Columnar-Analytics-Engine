// Columnar Analytics Engine
// Author: RIAL Fares
// Tests for file corruption resistance and input validation

#include "format.h"
#include "encoding.h"
#include <iostream>
#include <fstream>
#include <cassert>
#include <cstring>

using namespace columnar;

const char* TEST_FILE_TINY = "test_tiny.col";
const char* TEST_FILE_BAD_HEADER = "test_bad_header.col";
const char* TEST_FILE_BAD_FOOTER = "test_bad_footer.col";
const char* TEST_FILE_BAD_OFFSET = "test_bad_offset.col";

// Helper to create a minimal valid file
void createMinimalValidFile(const char* path) {
    std::ofstream file(path, std::ios::binary);

    // Header
    uint32_t file_magic = 0x454C4F43;  // COLE
    uint16_t major = 1, minor = 0;
    file.write(reinterpret_cast<const char*>(&file_magic), 4);
    file.write(reinterpret_cast<const char*>(&major), 2);
    file.write(reinterpret_cast<const char*>(&minor), 2);

    // Minimal metadata at offset 8
    uint32_t num_columns = 0;
    uint32_t num_row_groups = 0;
    uint32_t total_rows = 0;
    file.write(reinterpret_cast<const char*>(&num_columns), 4);
    file.write(reinterpret_cast<const char*>(&num_row_groups), 4);
    file.write(reinterpret_cast<const char*>(&total_rows), 4);

    // Footer
    uint32_t footer_magic = 0x464F4F54;  // FOOT
    uint64_t metadata_offset = 8;
    file.write(reinterpret_cast<const char*>(&footer_magic), 4);
    file.write(reinterpret_cast<const char*>(&metadata_offset), 8);

    file.close();
}

// Test 1: File too small (< 12 bytes)
void test_file_too_small() {
    {
        std::ofstream file(TEST_FILE_TINY, std::ios::binary);
        const char* data = "tiny";
        file.write(data, 4);
    }

    bool caught_exception = false;
    try {
        auto reader = std::make_shared<FileReader>(TEST_FILE_TINY);
    } catch (const std::exception& e) {
        std::string msg = e.what();
        caught_exception = (msg.find("too small") != std::string::npos ||
                           msg.find("minimum 12 bytes") != std::string::npos);
    }

    std::remove(TEST_FILE_TINY);
    assert(caught_exception && "Should reject file < 12 bytes");
    std::cout << "test_file_too_small: PASS\n";
}

// Test 2: Invalid header magic
void test_invalid_header_magic() {
    {
        std::ofstream file(TEST_FILE_BAD_HEADER, std::ios::binary);

        // Wrong magic
        uint32_t bad_magic = 0xDEADBEEF;
        uint16_t major = 1, minor = 0;
        file.write(reinterpret_cast<const char*>(&bad_magic), 4);
        file.write(reinterpret_cast<const char*>(&major), 2);
        file.write(reinterpret_cast<const char*>(&minor), 2);

        // Minimal metadata
        uint32_t num_columns = 0, num_row_groups = 0, total_rows = 0;
        file.write(reinterpret_cast<const char*>(&num_columns), 4);
        file.write(reinterpret_cast<const char*>(&num_row_groups), 4);
        file.write(reinterpret_cast<const char*>(&total_rows), 4);

        // Valid footer
        uint32_t footer_magic = 0x464F4F54;
        uint64_t metadata_offset = 8;
        file.write(reinterpret_cast<const char*>(&footer_magic), 4);
        file.write(reinterpret_cast<const char*>(&metadata_offset), 8);
    }

    bool caught_exception = false;
    try {
        auto reader = std::make_shared<FileReader>(TEST_FILE_BAD_HEADER);
    } catch (const std::exception& e) {
        std::string msg = e.what();
        caught_exception = (msg.find("Invalid file magic") != std::string::npos);
    }

    std::remove(TEST_FILE_BAD_HEADER);
    assert(caught_exception && "Should reject invalid header magic");
    std::cout << "test_invalid_header_magic: PASS\n";
}

// Test 3: Invalid footer magic
void test_invalid_footer_magic() {
    {
        std::ofstream file(TEST_FILE_BAD_FOOTER, std::ios::binary);

        // Valid header
        uint32_t file_magic = 0x454C4F43;
        uint16_t major = 1, minor = 0;
        file.write(reinterpret_cast<const char*>(&file_magic), 4);
        file.write(reinterpret_cast<const char*>(&major), 2);
        file.write(reinterpret_cast<const char*>(&minor), 2);

        // Minimal metadata
        uint32_t num_columns = 0, num_row_groups = 0, total_rows = 0;
        file.write(reinterpret_cast<const char*>(&num_columns), 4);
        file.write(reinterpret_cast<const char*>(&num_row_groups), 4);
        file.write(reinterpret_cast<const char*>(&total_rows), 4);

        // Bad footer
        uint32_t bad_footer = 0xBADFOOD0;
        uint64_t metadata_offset = 8;
        file.write(reinterpret_cast<const char*>(&bad_footer), 4);
        file.write(reinterpret_cast<const char*>(&metadata_offset), 8);
    }

    bool caught_exception = false;
    try {
        auto reader = std::make_shared<FileReader>(TEST_FILE_BAD_FOOTER);
    } catch (const std::exception& e) {
        std::string msg = e.what();
        caught_exception = (msg.find("Invalid footer magic") != std::string::npos);
    }

    std::remove(TEST_FILE_BAD_FOOTER);
    assert(caught_exception && "Should reject invalid footer magic");
    std::cout << "test_invalid_footer_magic: PASS\n";
}

// Test 4: Metadata offset out of bounds
void test_metadata_offset_out_of_bounds() {
    {
        std::ofstream file(TEST_FILE_BAD_OFFSET, std::ios::binary);

        // Valid header
        uint32_t file_magic = 0x454C4F43;
        uint16_t major = 1, minor = 0;
        file.write(reinterpret_cast<const char*>(&file_magic), 4);
        file.write(reinterpret_cast<const char*>(&major), 2);
        file.write(reinterpret_cast<const char*>(&minor), 2);

        // Minimal metadata
        uint32_t num_columns = 0, num_row_groups = 0, total_rows = 0;
        file.write(reinterpret_cast<const char*>(&num_columns), 4);
        file.write(reinterpret_cast<const char*>(&num_row_groups), 4);
        file.write(reinterpret_cast<const char*>(&total_rows), 4);

        // Footer with out-of-bounds offset
        uint32_t footer_magic = 0x464F4F54;
        uint64_t bad_offset = 999999999;  // Way beyond file size
        file.write(reinterpret_cast<const char*>(&footer_magic), 4);
        file.write(reinterpret_cast<const char*>(&bad_offset), 8);
    }

    bool caught_exception = false;
    try {
        auto reader = std::make_shared<FileReader>(TEST_FILE_BAD_OFFSET);
    } catch (const std::exception& e) {
        std::string msg = e.what();
        caught_exception = (msg.find("metadata offset") != std::string::npos ||
                           msg.find("beyond end of file") != std::string::npos);
    }

    std::remove(TEST_FILE_BAD_OFFSET);
    assert(caught_exception && "Should reject out-of-bounds metadata offset");
    std::cout << "test_metadata_offset_out_of_bounds: PASS\n";
}

// Test 5: Truncated varint
void test_truncated_varint() {
    uint8_t buffer[2] = {0x80, 0x80};  // Continuation bits set, but truncated
    size_t bytes_read = 0;

    bool caught_exception = false;
    try {
        VarintCodec::decodeUInt32Safe(buffer, 2, &bytes_read);
    } catch (const std::exception& e) {
        std::string msg = e.what();
        caught_exception = (msg.find("Truncated varint") != std::string::npos ||
                           msg.find("unexpected end") != std::string::npos);
    }

    assert(caught_exception && "Should reject truncated varint");
    std::cout << "test_truncated_varint: PASS\n";
}

// Test 6: Varint with infinite continuation bits (overflow)
void test_varint_overflow() {
    // 6 bytes with all continuation bits set (max is 5 for uint32)
    uint8_t buffer[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    size_t bytes_read = 0;

    bool caught_exception = false;
    try {
        VarintCodec::decodeUInt32Safe(buffer, 6, &bytes_read);
    } catch (const std::exception& e) {
        std::string msg = e.what();
        caught_exception = (msg.find("overflow") != std::string::npos ||
                           msg.find("more than 5 bytes") != std::string::npos);
    }

    assert(caught_exception && "Should reject varint overflow");
    std::cout << "test_varint_overflow: PASS\n";
}

int main() {
    std::cout << "Running corruption resistance tests...\n";

    test_file_too_small();
    test_invalid_header_magic();
    test_invalid_footer_magic();
    test_metadata_offset_out_of_bounds();
    test_truncated_varint();
    test_varint_overflow();

    std::cout << "\nAll corruption tests passed.\n";
    return 0;
}
