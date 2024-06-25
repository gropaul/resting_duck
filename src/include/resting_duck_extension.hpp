#pragma once

#include "duckdb.hpp"

namespace duckdb {

class RestingDuckExtension final : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;

private:
	static void RegisterTableFunctions(Catalog &catalog, ClientContext &context);
};

} // namespace duckdb
