// Columnar Analytics Engine
// Author: RIAL Fares
// Tests for execution engine

#include "format.h"
#include "execution.h"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <memory>

using namespace columnar;

const std::string TEST_FILE = "test_execution.col";

void cleanup() {
    if (std::filesystem::exists(TEST_FILE)) {
        std::filesystem::remove(TEST_FILE);
    }
}

void createTestFile() {
    Schema schema;
    schema.columns = {
        {"id", ColumnType::INT64, EncodingType::PLAIN},
        {"value", ColumnType::INT32, EncodingType::PLAIN},
        {"category", ColumnType::STRING, EncodingType::DICTIONARY}
    };

    FileWriter writer(TEST_FILE, schema);

    std::vector<int64_t> ids = {1, 2, 3, 4, 5};
    std::vector<int32_t> values = {100, 200, 150, 300, 250};
    std::vector<std::string> categories = {"A", "B", "A", "C", "B"};

    writer.writeInt64Column(0, ids);
    writer.writeInt32Column(1, values);
    writer.writeStringColumn(2, categories);
    writer.close();
}

void test_predicate_evaluation() {
    Predicate pred{"value", CompareOp::GT, 150};

    assert(pred.evaluate(200) == true);
    assert(pred.evaluate(100) == false);
    assert(pred.evaluate(150) == false);

    std::cout << "test_predicate_evaluation: PASS\n";
}

void test_predicate_skip_page() {
    PageStats stats;
    stats.min_int = 100;
    stats.max_int = 200;

    Predicate pred_gt{"value", CompareOp::GT, 250};
    assert(pred_gt.canSkipPage(stats) == true);

    Predicate pred_lt{"value", CompareOp::LT, 50};
    assert(pred_lt.canSkipPage(stats) == true);

    Predicate pred_in{"value", CompareOp::GT, 150};
    assert(pred_in.canSkipPage(stats) == false);

    std::cout << "test_predicate_skip_page: PASS\n";
}

void test_scanner_basic() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    Scanner scanner(reader, {"id", "value"});

    assert(scanner.hasNext());
    Batch batch = scanner.next();

    assert(batch.num_rows == 5);
    assert(batch.column_names.size() == 2);

    const auto& ids = batch.getColumn<int64_t>(0);
    const auto& values = batch.getColumn<int32_t>(1);

    assert(ids.size() == 5);
    assert(values.size() == 5);
    assert(ids[0] == 1 && ids[4] == 5);

    cleanup();
    std::cout << "test_scanner_basic: PASS\n";
}

void test_scanner_with_filter() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    Scanner scanner(reader, {"id", "value"});

    Predicate filter{"value", CompareOp::GT, 150};
    scanner.addFilter(filter);

    Batch batch = scanner.next();

    assert(batch.num_rows == 3);

    const auto& values = batch.getColumn<int32_t>(1);
    for (int32_t val : values) {
        assert(val > 150);
    }

    cleanup();
    std::cout << "test_scanner_with_filter: PASS\n";
}

void test_query_projection() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    QueryExecutor executor(reader);

    executor.setProjection({"value"});
    auto batches = executor.executeQuery();

    assert(!batches.empty());
    const auto& batch = batches[0];
    assert(batch.column_names.size() == 1);
    assert(batch.column_names[0] == "value");

    cleanup();
    std::cout << "test_query_projection: PASS\n";
}

void test_aggregation_count() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    QueryExecutor executor(reader);

    executor.setAggregation(AggFunc::COUNT, "id");
    auto result = executor.executeAggregate();

    assert(result.count == 5);

    cleanup();
    std::cout << "test_aggregation_count: PASS\n";
}

void test_aggregation_sum() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    QueryExecutor executor(reader);

    executor.setAggregation(AggFunc::SUM, "value");
    auto result = executor.executeAggregate();

    assert(result.count == 5);
    assert(result.sum == 1000);

    cleanup();
    std::cout << "test_aggregation_sum: PASS\n";
}

void test_aggregation_with_filter() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    QueryExecutor executor(reader);

    executor.addFilter(Predicate{"value", CompareOp::GT, 150});
    executor.setAggregation(AggFunc::COUNT, "id");

    auto result = executor.executeAggregate();
    assert(result.count == 3);

    cleanup();
    std::cout << "test_aggregation_with_filter: PASS\n";
}

void test_group_by() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    QueryExecutor executor(reader);

    executor.setGroupBy("category");
    executor.setAggregation(AggFunc::COUNT, "id");

    auto results = executor.executeGroupBy();

    assert(results.size() == 3);

    for (const auto& [key, agg] : results) {
        if (key == "A") {
            assert(agg.count == 2);
        } else if (key == "B") {
            assert(agg.count == 2);
        } else if (key == "C") {
            assert(agg.count == 1);
        }
    }

    cleanup();
    std::cout << "test_group_by: PASS\n";
}

void test_group_by_with_sum() {
    cleanup();
    createTestFile();

    auto reader = std::make_shared<FileReader>(TEST_FILE);
    QueryExecutor executor(reader);

    executor.setGroupBy("category");
    executor.setAggregation(AggFunc::SUM, "value");

    auto results = executor.executeGroupBy();

    for (const auto& [key, agg] : results) {
        if (key == "A") {
            assert(agg.sum == 250);
        } else if (key == "B") {
            assert(agg.sum == 450);
        } else if (key == "C") {
            assert(agg.sum == 300);
        }
    }

    cleanup();
    std::cout << "test_group_by_with_sum: PASS\n";
}

int main() {
    std::cout << "Running execution tests...\n";

    test_predicate_evaluation();
    test_predicate_skip_page();
    test_scanner_basic();
    test_scanner_with_filter();
    test_query_projection();
    test_aggregation_count();
    test_aggregation_sum();
    test_aggregation_with_filter();
    test_group_by();
    test_group_by_with_sum();

    std::cout << "\nAll execution tests passed.\n";
    return 0;
}
