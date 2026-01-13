// Columnar Analytics Engine
// Author: RIAL Fares
// Vectorized execution engine

#pragma once

#include "format.h"
#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <variant>
#include <functional>

namespace columnar {

// Vectorized batch of column data
struct Batch {
    using ColumnData = std::variant<
        std::vector<int32_t>,
        std::vector<int64_t>,
        std::vector<std::string>
    >;

    std::vector<ColumnData> columns;
    std::vector<std::string> column_names;
    size_t num_rows;

    template<typename T>
    const std::vector<T>& getColumn(size_t idx) const {
        return std::get<std::vector<T>>(columns[idx]);
    }

    size_t columnIndex(const std::string& name) const;
};

// Comparison operators for filters
enum class CompareOp {
    EQ,  // ==
    NE,  // !=
    LT,  // <
    LE,  // <=
    GT,  // >
    GE   // >=
};

// Predicate for filtering
struct Predicate {
    std::string column;
    CompareOp op;
    int64_t value;  // Only numeric predicates for MVP

    bool evaluate(int32_t col_value) const;
    bool evaluate(int64_t col_value) const;

    // Check if predicate can eliminate a page based on stats
    bool canSkipPage(const PageStats& stats) const;
};

// Aggregation functions
enum class AggFunc {
    COUNT,
    SUM,
    MIN,
    MAX
};

// Aggregation result
struct AggResult {
    int64_t count;
    int64_t sum;
    std::optional<int64_t> min;
    std::optional<int64_t> max;
};

// Scanner: reads batches from file with optional filters
class Scanner {
public:
    Scanner(std::shared_ptr<FileReader> reader,
            std::vector<std::string> columns,
            size_t batch_size = 4096);

    void addFilter(Predicate pred);
    bool hasNext();
    Batch next();

private:
    std::shared_ptr<FileReader> reader_;
    std::vector<std::string> selected_columns_;
    std::vector<size_t> column_indices_;
    std::vector<Predicate> filters_;
    size_t batch_size_;
    size_t current_row_group_;
    size_t current_offset_;
};

// Query executor
class QueryExecutor {
public:
    explicit QueryExecutor(std::shared_ptr<FileReader> reader);

    // Configure query
    void setProjection(std::vector<std::string> columns);
    void addFilter(Predicate pred);
    void setAggregation(AggFunc func, std::string column);
    void setGroupBy(std::string column);

    // Execute and return results
    std::vector<Batch> executeQuery();
    AggResult executeAggregate();
    std::vector<std::pair<std::string, AggResult>> executeGroupBy();

private:
    std::shared_ptr<FileReader> reader_;
    std::vector<std::string> projection_;
    std::vector<Predicate> filters_;
    std::optional<std::pair<AggFunc, std::string>> aggregation_;
    std::optional<std::string> group_by_column_;
};

} // namespace columnar
