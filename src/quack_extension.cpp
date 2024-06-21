#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "const.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "functions.hpp"
#include "sql_runner.hpp"

namespace duckdb {

void QuackExtension::Load(DuckDB &db) {
  Connection conn(*db.instance);
  conn.BeginTransaction();

  auto &context = *conn.context;
  auto &catalog = Catalog::GetSystemCatalog(context);

  RegisterTableFunctions(catalog, context);

  conn.Commit();
}

void QuackExtension::RegisterTableFunctions(Catalog &catalog,
                                            ClientContext &context) {
  const TableFunction tf(JSON_RESULT, {LogicalTypeId::VARCHAR}, JsonResultTf,
                         JsonResultFunctionDataBind,
                         JsonResultTableFunctionState::Init);
  CreateTableFunctionInfo tf_info(tf);
  tf_info.name = JSON_RESULT;
  catalog.CreateTableFunction(context, &tf_info);
}

std::string QuackExtension::Name() { return "restring_duck"; }

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
  return EXT_VERSION_QUACK;
#else
  return "extension-version-not-set";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quack_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::QuackExtension>();
}

DUCKDB_EXTENSION_API const char *quack_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
