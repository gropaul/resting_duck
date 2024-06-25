#pragma once
#include "result_serializer.hpp"

namespace duckdb {

struct JsonResultFunction final : FunctionData {
	const string query;
	const bool set_invalid_values_to_null;
	explicit JsonResultFunction(string query, const bool _set_invalid_values_to_null)
	    : query(std::move(query)), set_invalid_values_to_null(_set_invalid_values_to_null) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<JsonResultFunction>(query, set_invalid_values_to_null);
	}

	bool Equals(const FunctionData &other) const override {
		return query == other.Cast<JsonResultFunction>().query;
	}
};

struct JsonResultTableFunctionState final : GlobalTableFunctionState {
	JsonResultTableFunctionState() : run(false) {};
	std::atomic_bool run;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return duckdb::make_uniq<JsonResultTableFunctionState>();
	}
};

inline unique_ptr<FunctionData> JsonResultFunctionDataBind(
    ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types, vector<string> &names
) {
	// Check if set_invalid_values_to_null is set
	const auto set_invalid_values_to_null =
	    input.named_parameters.find("set_invalid_values_to_null") != input.named_parameters.end()
	        ? input.named_parameters.at("set_invalid_values_to_null").GetValue<bool>()
	        : false;

	auto function_data = make_uniq<JsonResultFunction>(input.inputs[0].GetValue<string>(), set_invalid_values_to_null);

	types = {LogicalType::BOOLEAN, LogicalType::VARCHAR};
	names = {"success", "data"};

	return unique_ptr_cast<JsonResultFunction, FunctionData>(std::move(function_data));
}

inline void JsonResultTf(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<JsonResultTableFunctionState>();
	if (state.run.exchange(true)) {
		return;
	}

	Connection conn(*context.db);
	auto &function_data = data.bind_data->Cast<JsonResultFunction>();
	auto result = conn.Query(function_data.query);
	if (result->HasError()) {
		result->ThrowError();
	}

	ResultSerializer serializer {function_data.set_invalid_values_to_null};
	serializer.Serialize(std::move(result));

	auto &serialization_result = *serializer.result;
	if (!serialization_result.IsSuccess()) {
		throw SerializationException("Failed to serialize result: " + serialization_result.Raw());
	}
	output.SetValue(0, 0, Value(serialization_result.IsSuccess()));
	output.SetValue(1, 0, Value(serialization_result.Raw()));
	output.SetCardinality(1);
}

} // namespace duckdb
