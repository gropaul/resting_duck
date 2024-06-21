#pragma once

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

#include <result_serializer.hpp>

namespace duckdb {

class SQLRunner {
public:
  explicit SQLRunner(DatabaseInstance &_instance) : instance(_instance) {}

  unique_ptr<QueryResult> RunSQL(const string &query) const {
    Connection con(instance);
    return con.Query(query);
  }

  std::unique_ptr<SerializationResult>
  RunSQLAndSerialize(const string &query) const {
    auto result = RunSQL(query);
    ResultSerializer serializer;
    serializer.Serialize(std::move(result));
    return std::move(serializer.result);
  }

private:
  DatabaseInstance &instance;
};

} // namespace duckdb