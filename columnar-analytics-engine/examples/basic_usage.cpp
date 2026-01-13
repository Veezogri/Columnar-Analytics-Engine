// Columnar Analytics Engine
// Author: RIAL Fares
// Basic usage example

#include "format.h"
#include "execution.h"
#include <iostream>
#include <memory>

using namespace columnar;

int main() {
    // Define schema
    Schema schema;
    schema.columns = {
        {"id", ColumnType::INT64, EncodingType::PLAIN},
        {"age", ColumnType::INT32, EncodingType::PLAIN},
        {"city", ColumnType::STRING, EncodingType::DICTIONARY}
    };

    // Write data
    {
        FileWriter writer("example.col", schema);

        std::vector<int64_t> ids = {1, 2, 3, 4, 5};
        std::vector<int32_t> ages = {25, 30, 25, 35, 30};
        std::vector<std::string> cities = {"Paris", "Lyon", "Paris", "Nice", "Lyon"};

        writer.writeInt64Column(0, ids);
        writer.writeInt32Column(1, ages);
        writer.writeStringColumn(2, cities);

        writer.close();
    }

    std::cout << "Data written to example.col\n\n";

    // Read data
    {
        auto reader = std::make_shared<FileReader>("example.col");

        std::cout << "Schema:\n";
        for (const auto& col : reader->schema().columns) {
            std::cout << "  - " << col.name << "\n";
        }
        std::cout << "\nTotal rows: " << reader->metadata().total_rows << "\n\n";

        // Query 1: Full scan
        std::cout << "Query 1: SELECT * FROM data\n";
        {
            QueryExecutor executor(reader);
            auto batches = executor.executeQuery();

            for (const auto& batch : batches) {
                std::cout << "  Returned " << batch.num_rows << " rows\n";
            }
        }
        std::cout << "\n";

        // Query 2: Filter
        std::cout << "Query 2: SELECT * FROM data WHERE age > 25\n";
        {
            QueryExecutor executor(reader);
            executor.addFilter(Predicate{"age", CompareOp::GT, 25});
            auto batches = executor.executeQuery();

            size_t total = 0;
            for (const auto& batch : batches) {
                total += batch.num_rows;
            }
            std::cout << "  Returned " << total << " rows\n";
        }
        std::cout << "\n";

        // Query 3: Aggregation
        std::cout << "Query 3: SELECT COUNT(*), SUM(age) FROM data\n";
        {
            QueryExecutor executor(reader);
            executor.setAggregation(AggFunc::SUM, "age");
            auto result = executor.executeAggregate();

            std::cout << "  Count: " << result.count << "\n";
            std::cout << "  Sum: " << result.sum << "\n";
        }
        std::cout << "\n";

        // Query 4: Group by
        std::cout << "Query 4: SELECT city, COUNT(*) FROM data GROUP BY city\n";
        {
            QueryExecutor executor(reader);
            executor.setGroupBy("city");
            executor.setAggregation(AggFunc::COUNT, "id");
            auto results = executor.executeGroupBy();

            for (const auto& [city, agg] : results) {
                std::cout << "  " << city << ": " << agg.count << "\n";
            }
        }
    }

    return 0;
}
