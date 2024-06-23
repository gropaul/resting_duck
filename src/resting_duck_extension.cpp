#define DUCKDB_EXTENSION_MAIN

#include "resting_duck_extension.hpp"
#include "const.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "functions.hpp"
#include "sql_runner.hpp"

#include "server.hpp"

namespace duckdb {

void RestingDuckExtension::Load(DuckDB &db) {
  Connection conn(*db.instance);
  conn.BeginTransaction();

  auto &context = *conn.context;
  auto &catalog = Catalog::GetSystemCatalog(context);

  RegisterTableFunctions(catalog, context);

  start_server();

  conn.Commit();
}

void RestingDuckExtension::RegisterTableFunctions(Catalog &catalog,
                                                  ClientContext &context) {
  const TableFunction tf(JSON_RESULT, {LogicalTypeId::VARCHAR}, JsonResultTf,
                         JsonResultFunctionDataBind,
                         JsonResultTableFunctionState::Init);
  CreateTableFunctionInfo tf_info(tf);
  tf_info.name = JSON_RESULT;
  catalog.CreateTableFunction(context, &tf_info);
}

std::string RestingDuckExtension::Name() { return "restring_duck"; }

std::string RestingDuckExtension::Version() const {
#ifdef EXT_VERSION_RESTING_DUCK
  return EXT_VERSION_RESTING_DUCK;
#else
  return "extension-version-not-set";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void resting_duck_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::RestingDuckExtension>();
}

DUCKDB_EXTENSION_API const char *resting_duck_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
