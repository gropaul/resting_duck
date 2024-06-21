#pragma once
#include "result_serializer.hpp"

namespace duckdb {

struct JsonResultFunction final : FunctionData {
  string query;
  explicit JsonResultFunction(string query) : query(std::move(query)) {}

  unique_ptr<FunctionData> Copy() const override {
    return make_uniq<JsonResultFunction>(query);
  }

  bool Equals(const FunctionData &other) const override {
    return query == other.Cast<JsonResultFunction>().query;
  }
};

struct JsonResultTableFunctionState final : GlobalTableFunctionState {
  JsonResultTableFunctionState() : run(false){};
  std::atomic_bool run;

  static unique_ptr<GlobalTableFunctionState> Init(ClientContext &,
                                                   TableFunctionInitInput &) {
    return duckdb::make_uniq<JsonResultTableFunctionState>();
  }
};

inline unique_ptr<FunctionData>
JsonResultFunctionDataBind(ClientContext &,
                           TableFunctionBindInput &input,
                           vector<LogicalType> &types, vector<string> &names) {
  auto function_data =
      make_uniq<JsonResultFunction>(input.inputs[0].GetValue<string>());

  types = {LogicalType::BOOLEAN, LogicalType::VARCHAR};
  names = {"success", "data"};

  return unique_ptr_cast<JsonResultFunction, FunctionData>(
      std::move(function_data));
}

inline void JsonResultTf(ClientContext &context, TableFunctionInput &data,
                         DataChunk &output) {
  auto &state = data.global_state->Cast<JsonResultTableFunctionState>();
  if (state.run.exchange(true)) {
    return;
  }

  Connection conn(*context.db);
  auto result = conn.Query(data.bind_data->Cast<JsonResultFunction>().query);
  ResultSerializer serializer;
  serializer.Serialize(std::move(result));

  auto &serialization_result = *serializer.result;

  output.SetValue(0, 0, Value(serialization_result.IsSuccess()));
  output.SetValue(0, 1, Value(serialization_result.WithSuccessField()));
  output.SetCardinality(1);
}

} // namespace duckdb
