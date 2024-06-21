#pragma once
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/query_result.hpp"
#include "yyjson.hpp"

namespace duckdb {
using namespace duckdb_yyjson;

class SerializationResult {
public:
  virtual ~SerializationResult() { yyjson_mut_doc_free(doc); }
  virtual bool IsSuccess() = 0;
  virtual string WithSuccessField() = 0;
  virtual string Raw() = 0;

  template <class TARGET> TARGET &Cast() {
    DynamicCastCheck<TARGET>(this);
    return reinterpret_cast<TARGET &>(*this);
  }

private:
  yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
  yyjson_mut_val *root = yyjson_mut_obj(doc);
};

class SerializationSuccess final : public SerializationResult {
public:
  explicit SerializationSuccess(string serialized)
      : serialized(std::move(serialized)) {}

  bool IsSuccess() override { return true; }

  string Raw() override { return serialized; }

  string WithSuccessField() override {
    return R"({"success": true, "data": )" + serialized + "}";
  }

private:
  string serialized;
};

class SerializationError final : public SerializationResult {
public:
  explicit SerializationError(string message) : message(std::move(message)) {}

  bool IsSuccess() override { return false; }

  string Raw() override { return message; }
  string WithSuccessField() override {
    return R"({"success": false, "message": ")" + message + "\"}";
  }

private:
  string message;
};

class ResultSerializer {
public:
  unique_ptr<SerializationResult> result;

  ~ResultSerializer() { yyjson_mut_doc_free(doc); }

  void Serialize(unique_ptr<QueryResult> query) {
    try {
      auto serialized = SerializeInternal(std::move(query));
      result = make_uniq<SerializationSuccess>(serialized);
    } catch (const std::exception &e) {
      result = make_uniq<SerializationError>(e.what());
    }
  }

private:
  // ReSharper disable once CppPassValueParameterByConstReference
  string SerializeInternal(unique_ptr<QueryResult> query_result) {

    auto chunk = query_result->Fetch();
    auto names = query_result->names;
    auto types = query_result->types;
    while (chunk) {
      SerializeChunk(*chunk, names, types);

      chunk = query_result->Fetch();
    }

    char *json_str = yyjson_mut_write(doc, 0, nullptr);
    if (!json_str) {
      throw InternalException("Could not serialize yyjson document");
    }

    string result_str = json_str;
    free(json_str);
    return result_str;
  }

  void SerializeChunk(const DataChunk &chunk, vector<string> &names,
                      vector<LogicalType> &types) {
    const auto column_count = chunk.ColumnCount();
    const auto row_count = chunk.size();

    for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {

      // Which itself contains an object
      // ReSharper disable once CppLocalVariableMayBeConst
      auto obj = yyjson_mut_obj(doc);

      for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {

        auto value = chunk.GetValue(col_idx, row_idx);
        auto name = names[col_idx];
        auto type = types[col_idx];
        SerializeValue(obj, value, name, type);
      }
      yyjson_mut_arr_add_val(root, obj);
    }
  }

  // ReSharper disable once CppMemberFunctionMayBeConst
  void SerializeValue(yyjson_mut_val *parent, const Value &value,
                      optional_ptr<string> name, const LogicalType &type) {

    if (value.IsNull()) {
      goto null_handle;
    }

    switch (type.id()) {
    case LogicalTypeId::SQLNULL:
    null_handle:
      if (name) {
        yyjson_mut_obj_add_null(doc, parent, name->c_str());
      } else {
        yyjson_mut_arr_add_null(doc, parent);
      }
      break;
    case LogicalTypeId::BOOLEAN:
      if (name) {
        yyjson_mut_obj_add_bool(doc, parent, name->c_str(),
                                value.GetValue<bool>());
      } else {
        yyjson_mut_arr_add_bool(doc, parent, value.GetValue<bool>());
      }
      break;
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::INTEGER_LITERAL:
      if (name) {
        yyjson_mut_obj_add_int(doc, parent, name->c_str(),
                               value.GetValue<int64_t>());
      } else {
        yyjson_mut_arr_add_int(doc, parent, value.GetValue<int64_t>());
      }
      break;
    case LogicalTypeId::BIT:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::HUGEINT:
      if (name) {
        yyjson_mut_obj_add_uint(doc, parent, name->c_str(),
                                value.GetValue<uint64_t>());
      } else {
        yyjson_mut_arr_add_uint(doc, parent, value.GetValue<uint64_t>());
      }
      break;
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::DECIMAL:
      if (name) {
        yyjson_mut_obj_add_real(doc, parent, name->c_str(),
                                value.GetValue<double>());
      } else {
        yyjson_mut_arr_add_real(doc, parent, value.GetValue<double>());
      }
      break;
      // Data + time
    case LogicalTypeId::DATE:
    case LogicalTypeId::TIME:
    case LogicalTypeId::TIMESTAMP_SEC:
    case LogicalTypeId::TIMESTAMP_MS:
    case LogicalTypeId::TIMESTAMP:
    case LogicalTypeId::TIMESTAMP_NS:
    case LogicalTypeId::TIMESTAMP_TZ:
    case LogicalTypeId::TIME_TZ:
      // Enum
    case LogicalTypeId::ENUM:
      // Strings
    case LogicalTypeId::CHAR:
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::STRING_LITERAL:
      if (name) {
        yyjson_mut_obj_add_str(doc, parent, name->c_str(),
                               value.GetValue<string>().c_str());
      } else {
        yyjson_mut_arr_add_str(doc, parent, value.GetValue<string>().c_str());
      }
      break;
    case LogicalTypeId::UUID: {
      const auto uuid_int = value.GetValue<uhugeint_t>();
      const auto uuid = UUID::ToString(uuid_int);
      if (name) {
        yyjson_mut_obj_add_str(doc, parent, name->c_str(), uuid.c_str());
      } else {
        yyjson_mut_arr_add_str(doc, parent, uuid.c_str());
      }
      break;
    }
      // Not implemented types
    case LogicalTypeId::LIST:
    case LogicalTypeId::STRUCT:
    case LogicalTypeId::MAP: // Apparentely a list of structs...
    case LogicalTypeId::ARRAY:
    case LogicalTypeId::UNION:
      throw std::runtime_error("Type " + type.ToString() + " not implemented");
      // Unsupported types
    case LogicalTypeId::TABLE:
    case LogicalTypeId::POINTER:
    case LogicalTypeId::VALIDITY:
    case LogicalTypeId::AGGREGATE_STATE:
    case LogicalTypeId::LAMBDA:
    case LogicalTypeId::INTERVAL:
    case LogicalTypeId::BLOB:
    case LogicalTypeId::USER:
    case LogicalTypeId::ANY:
    case LogicalTypeId::UNKNOWN:
    case LogicalTypeId::INVALID:
      throw std::runtime_error("Type " + type.ToString() + " not supported");
    }
  }

  yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
  yyjson_mut_val *root = yyjson_mut_arr(doc);
};
} // namespace duckdb
