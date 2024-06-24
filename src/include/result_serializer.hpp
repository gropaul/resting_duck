#pragma once

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/query_result.hpp"
#include "yyjson.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include <iostream>

namespace duckdb {
using namespace duckdb_yyjson;

class SerializationResult {
public:
  virtual ~SerializationResult() = default;
  virtual bool IsSuccess() = 0;
  virtual string WithSuccessField() = 0;
  virtual string Raw() = 0;

  void Print() { std::cerr << WithSuccessField() << std::endl; }

  template <class TARGET> TARGET &Cast() {
    DynamicCastCheck<TARGET>(this);
    return reinterpret_cast<TARGET &>(*this);
  }
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
  ResultSerializer() {
    doc = yyjson_mut_doc_new(nullptr);
    root = yyjson_mut_arr(doc);
    if (!root) {
      throw InternalException("Could not create yyjson array");
    }
    yyjson_mut_doc_set_root(doc, root);
  }
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
        auto &name = names[col_idx];
        auto &type = types[col_idx];
        SerializeValue(obj, value, name, type);
      }
      if (!yyjson_mut_arr_append(root, obj)) {
        throw InternalException("Could not add object to yyjson array");
      }
    }
  }

  // ReSharper disable once CppMemberFunctionMayBeConst
  void SerializeValue(yyjson_mut_val *parent, const Value &value,
                      optional_ptr<string> name, const LogicalType &type) {
    yyjson_mut_val *val = nullptr;

    if (value.IsNull()) {
      goto null_handle;
    }

    switch (type.id()) {
    case LogicalTypeId::SQLNULL:
    null_handle:
      val = yyjson_mut_null(doc);
      break;
    case LogicalTypeId::BOOLEAN:
      val = yyjson_mut_bool(doc, value.GetValue<bool>());
      break;
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::INTEGER_LITERAL:
      val = yyjson_mut_int(doc, value.GetValue<int64_t>());
      break;
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
      val = yyjson_mut_uint(doc, value.GetValue<uint64_t>());
      break;
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::DECIMAL:
      val = yyjson_mut_real(doc, value.GetValue<double>());
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
      val = yyjson_mut_strcpy(doc, value.GetValue<string>().c_str());
      break;
    case LogicalTypeId::UUID: {
      const auto uuid_int = value.GetValue<hugeint_t>();
      const auto uuid = UUID::ToString(uuid_int);
      val = yyjson_mut_strcpy(doc, uuid.c_str());
      break;
    }
      // Weird special types that are jus serialized to string
    case LogicalTypeId::INTERVAL:
      // TODO perhaps base64 encode blob?
    case LogicalTypeId::BLOB:
    case LogicalTypeId::BIT:
      val = yyjson_mut_strcpy(doc, value.ToString().c_str());
      break;
    case LogicalTypeId::UNION: {
      auto &union_val = UnionValue::GetValue(value);
      SerializeValue(parent, union_val, name, union_val.type());
      return;
    }
    case LogicalTypeId::ARRAY:
    case LogicalTypeId::LIST: {
      const auto get_children = LogicalTypeId::LIST == type.id()
                                    ? ListValue::GetChildren
                                    : ArrayValue::GetChildren;
      auto &children = get_children(value);
      val = yyjson_mut_arr(doc);
      for (auto &child : children) {
        SerializeValue(val, child, nullptr, child.type());
      }
      break;
    }
    case LogicalTypeId::STRUCT: {
      const auto &children = StructValue::GetChildren(value);
      const auto &type_info = value.type().AuxInfo()->Cast<StructTypeInfo>();
      val = yyjson_mut_obj(doc);
      for (uint64_t idx = 0; idx < children.size(); ++idx) {
        string struct_name = type_info.child_types[idx].first;
        SerializeValue(val, children[idx], struct_name,
                       type_info.child_types[idx].second);
      }
      break;
    }
      // Not implemented types
    case LogicalTypeId::MAP: {
      auto &children = ListValue::GetChildren(value);
      val = yyjson_mut_obj(doc);
      for (auto &item : children) {
        auto &key_value = StructValue::GetChildren(item);
        for (uint64_t idx = 0; idx < key_value.size(); ++idx) {
          const auto &map_value = item.type().AuxInfo()->Cast<StructTypeInfo>();

          string struct_name = map_value.child_types[idx].first;
          SerializeValue(val, key_value[idx], struct_name,
                         map_value.child_types[idx].second);
          // TODO not quite right...
        }
      }
      break;
    }
    case LogicalTypeId::TABLE:
      throw InternalException("Type " + type.ToString() + " not implemented");
      // Unsupported types
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::HUGEINT:
    case LogicalTypeId::POINTER:
    case LogicalTypeId::VALIDITY:
    case LogicalTypeId::AGGREGATE_STATE:
    case LogicalTypeId::LAMBDA:
    case LogicalTypeId::USER:
    case LogicalTypeId::ANY:
    case LogicalTypeId::UNKNOWN:
    case LogicalTypeId::INVALID:
      throw InternalException("Type " + type.ToString() + " not supported");
    }

    D_ASSERT(val);
    if (!name) {
      if (!yyjson_mut_arr_append(parent, val)) {
        throw InternalException("Could not add value to yyjson array");
      }
    } else {
      yyjson_mut_val *key = yyjson_mut_strcpy(doc, name->c_str());
      D_ASSERT(key);
      if (!yyjson_mut_obj_add(parent, key, val)) {
        throw InternalException("Could not add value to yyjson object");
      }
    }
  }

  yyjson_mut_doc *doc;
  yyjson_mut_val *root;
};
} // namespace duckdb
