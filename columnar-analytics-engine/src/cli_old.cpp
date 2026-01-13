// Columnar Analytics Engine
// Author: RIAL Fares
// Command-line interface

#include "format.h"
#include "execution.h"
#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <cstring>
#include <algorithm>

using namespace columnar;

void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " <command> [options]\n\n";
    std::cerr << "Commands:\n";
    std::cerr << "  write <output.col> <num_rows> [seed]  - Generate and write synthetic dataset\n";
    std::cerr << "  scan <input.col>                      - Display file metadata and stats\n";
    std::cerr << "  query <input.col> [options]           - Execute query\n";
    std::cerr << "\nQuery options:\n";
    std::cerr << "  --select <col1,col2,...>              - Project specific columns\n";
    std::cerr << "  --where <column> <op> <value>         - Filter (op: eq, lt, le, gt, ge)\n";
    std::cerr << "  --agg <func> <column>                 - Aggregate (func: count, sum, min, max)\n";
    std::cerr << "  --groupby <column>                    - Group by column\n";
}

Schema createSyntheticSchema() {
    Schema schema;
    ColumnSchema col1;
    col1.name = "id";
    col1.type = ColumnType::INT64;
    col1.encoding = EncodingType::PLAIN;
    schema.columns.push_back(col1);

    ColumnSchema col2;
    col2.name = "value";
    col2.type = ColumnType::INT64;
    col2.encoding = EncodingType::DELTA;
    schema.columns.push_back(col2);

    ColumnSchema col3;
    col3.name = "category";
    col3.type = ColumnType::INT32;
    col3.encoding = EncodingType::RLE;
    schema.columns.push_back(col3);

    ColumnSchema col4;
    col4.name = "region";
    col4.type = ColumnType::STRING;
    col4.encoding = EncodingType::DICTIONARY;
    schema.columns.push_back(col4);

    ColumnSchema col5;
    col5.name = "status";
    col5.type = ColumnType::STRING;
    col5.encoding = EncodingType::DICTIONARY;
    schema.columns.push_back(col5);

    return schema;
}

void generateSyntheticData(const std::string& output_path, size_t num_rows, unsigned int seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> category_dist(1, 5);
    std::uniform_int_distribution<int64_t> value_dist(0, 10000);
    std::uniform_int_distribution<int> region_dist(0, 3);
    std::uniform_int_distribution<int> status_dist(0, 2);

    std::vector<std::string> regions;
    regions.push_back("north");
    regions.push_back("south");
    regions.push_back("east");
    regions.push_back("west");

    std::vector<std::string> statuses;
    statuses.push_back("active");
    statuses.push_back("pending");
    statuses.push_back("closed");

    Schema schema = createSyntheticSchema();
    FileWriter writer(output_path, schema);

    const size_t chunk_size = 10000;
    size_t remaining = num_rows;

    while (remaining > 0) {
        size_t current_chunk = std::min(remaining, chunk_size);

        std::vector<int64_t> ids(current_chunk);
        std::vector<int64_t> values(current_chunk);
        std::vector<int32_t> categories(current_chunk);
        std::vector<std::string> region_vals(current_chunk);
        std::vector<std::string> status_vals(current_chunk);

        for (size_t i = 0; i < current_chunk; i++) {
            ids[i] = static_cast<int64_t>(num_rows - remaining + i);
            values[i] = value_dist(rng);
            categories[i] = category_dist(rng);
            region_vals[i] = regions[region_dist(rng)];
            status_vals[i] = statuses[status_dist(rng)];
        }

        writer.writeInt64Column(0, ids);
        writer.writeInt64Column(1, values);
        writer.writeInt32Column(2, categories);
        writer.writeStringColumn(3, region_vals);
        writer.writeStringColumn(4, status_vals);

        writer.flushRowGroup();

        remaining -= current_chunk;
    }

    writer.close();
    std::cout << "Generated " << num_rows << " rows in " << output_path << "\n";
}

void scanFile(const std::string& input_path) {
    FileReader reader(input_path);
    const auto& metadata = reader.metadata();

    std::cout << "File: " << input_path << "\n";
    std::cout << "Total rows: " << metadata.total_rows << "\n";
    std::cout << "Row groups: " << metadata.row_groups.size() << "\n\n";

    std::cout << "Schema:\n";
    for (const auto& col : metadata.schema.columns) {
        std::cout << "  - " << col.name << " (type=";
        switch (col.type) {
        case ColumnType::INT32: std::cout << "INT32"; break;
        case ColumnType::INT64: std::cout << "INT64"; break;
        case ColumnType::STRING: std::cout << "STRING"; break;
        }
        std::cout << ", encoding=";
        switch (col.encoding) {
        case EncodingType::PLAIN: std::cout << "PLAIN"; break;
        case EncodingType::RLE: std::cout << "RLE"; break;
        case EncodingType::DELTA: std::cout << "DELTA"; break;
        case EncodingType::DICTIONARY: std::cout << "DICTIONARY"; break;
        }
        std::cout << ")\n";
    }

    std::cout << "\nRow Groups:\n";
    for (size_t i = 0; i < metadata.row_groups.size(); i++) {
        const auto& rg = metadata.row_groups[i];
        std::cout << "  Row Group " << i << ": " << rg.num_rows << " rows\n";

        for (size_t j = 0; j < rg.column_chunks.size(); j++) {
            const auto& cc = rg.column_chunks[j];
            std::cout << "    Column " << metadata.schema.columns[j].name << ":\n";
            std::cout << "      Offset: " << cc.file_offset << "\n";
            std::cout << "      Size: " << cc.total_size << " bytes\n";

            for (size_t k = 0; k < cc.page_headers.size(); k++) {
                const auto& ph = cc.page_headers[k];
                std::cout << "      Page " << k << ": " << ph.num_values << " values, ";
                std::cout << ph.compressed_size << " bytes";

                if (ph.stats.min_int.has_value() && ph.stats.max_int.has_value()) {
                    std::cout << ", min=" << ph.stats.min_int.value();
                    std::cout << ", max=" << ph.stats.max_int.value();
                }
                std::cout << "\n";
            }
        }
    }
}

CompareOp parseCompareOp(const std::string& op) {
    if (op == "eq") return CompareOp::EQ;
    if (op == "ne") return CompareOp::NE;
    if (op == "lt") return CompareOp::LT;
    if (op == "le") return CompareOp::LE;
    if (op == "gt") return CompareOp::GT;
    if (op == "ge") return CompareOp::GE;
    throw std::runtime_error("Invalid comparison operator: " + op);
}

AggFunc parseAggFunc(const std::string& func) {
    if (func == "count") return AggFunc::COUNT;
    if (func == "sum") return AggFunc::SUM;
    if (func == "min") return AggFunc::MIN;
    if (func == "max") return AggFunc::MAX;
    throw std::runtime_error("Invalid aggregation function: " + func);
}

std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    for (char c : str) {
        if (c == delimiter) {
            if (!token.empty()) {
                tokens.push_back(token);
                token.clear();
            }
        } else {
            token += c;
        }
    }
    if (!token.empty()) {
        tokens.push_back(token);
    }
    return tokens;
}

void executeQuery(int argc, char* argv[]) {
    if (argc < 3) {
        printUsage(argv[0]);
        return;
    }

    std::string input_path = argv[2];
    auto reader = std::make_shared<FileReader>(input_path);

    QueryExecutor executor(reader);
    std::vector<std::string> projection;
    std::optional<std::pair<AggFunc, std::string>> aggregation;
    std::optional<std::string> group_by;

    for (int i = 3; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--select" && i + 1 < argc) {
            projection = split(argv[++i], ',');
            executor.setProjection(projection);
        } else if (arg == "--where" && i + 3 < argc) {
            std::string col = argv[++i];
            std::string op = argv[++i];
            int64_t value = std::stoll(argv[++i]);
            executor.addFilter(Predicate{col, parseCompareOp(op), value});
        } else if (arg == "--agg" && i + 2 < argc) {
            std::string func = argv[++i];
            std::string col = argv[++i];
            aggregation = std::make_pair(parseAggFunc(func), col);
            executor.setAggregation(aggregation->first, aggregation->second);
        } else if (arg == "--groupby" && i + 1 < argc) {
            group_by = argv[++i];
            executor.setGroupBy(group_by.value());
        }
    }

    if (group_by.has_value()) {
        auto results = executor.executeGroupBy();
        std::cout << "GROUP BY " << group_by.value() << ":\n";
        for (const auto& [key, agg] : results) {
            std::cout << "  " << key << ": count=" << agg.count;
            if (agg.sum != 0 || aggregation.has_value()) {
                std::cout << ", sum=" << agg.sum;
            }
            std::cout << "\n";
        }
    } else if (aggregation.has_value()) {
        auto result = executor.executeAggregate();
        std::cout << "Aggregation result:\n";
        std::cout << "  count: " << result.count << "\n";
        if (aggregation->first != AggFunc::COUNT) {
            std::cout << "  sum: " << result.sum << "\n";
            if (result.min.has_value()) {
                std::cout << "  min: " << result.min.value() << "\n";
            }
            if (result.max.has_value()) {
                std::cout << "  max: " << result.max.value() << "\n";
            }
        }
    } else {
        auto batches = executor.executeQuery();
        size_t total_rows = 0;
        for (const auto& batch : batches) {
            total_rows += batch.num_rows;
        }
        std::cout << "Query returned " << total_rows << " rows in " << batches.size() << " batches\n";

        if (!batches.empty() && total_rows <= 20) {
            std::cout << "\nFirst rows:\n";
            for (const auto& batch : batches) {
                for (size_t row = 0; row < std::min(batch.num_rows, size_t(20)); row++) {
                    for (size_t col = 0; col < batch.columns.size(); col++) {
                        if (col > 0) std::cout << ", ";
                        std::cout << batch.column_names[col] << "=";

                        const auto& col_data = batch.columns[col];
                        if (std::holds_alternative<std::vector<int32_t>>(col_data)) {
                            std::cout << std::get<std::vector<int32_t>>(col_data)[row];
                        } else if (std::holds_alternative<std::vector<int64_t>>(col_data)) {
                            std::cout << std::get<std::vector<int64_t>>(col_data)[row];
                        } else if (std::holds_alternative<std::vector<std::string>>(col_data)) {
                            std::cout << std::get<std::vector<std::string>>(col_data)[row];
                        }
                    }
                    std::cout << "\n";
                }
            }
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }

    std::string command = argv[1];

    try {
        if (command == "write") {
            if (argc < 4) {
                printUsage(argv[0]);
                return 1;
            }

            std::string output = argv[2];
            size_t num_rows = std::stoull(argv[3]);
            unsigned int seed = argc >= 5 ? std::stoul(argv[4]) : 42;

            generateSyntheticData(output, num_rows, seed);

        } else if (command == "scan") {
            if (argc < 3) {
                printUsage(argv[0]);
                return 1;
            }

            scanFile(argv[2]);

        } else if (command == "query") {
            executeQuery(argc, argv);

        } else {
            std::cerr << "Unknown command: " << command << "\n";
            printUsage(argv[0]);
            return 1;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
