
#include "oatpp/network/Server.hpp"
#include "oatpp/network/tcp/server/ConnectionProvider.hpp"
#include "oatpp/web/server/HttpConnectionHandler.hpp"


namespace duckdb {

std::string url_decode(const std::string &value) {
	std::string result;
	result.reserve(value.size());

	for (std::size_t i = 0; i < value.size(); ++i) {
		if (value[i] == '%' && i + 2 < value.size()) {
			std::string hex = value.substr(i + 1, 2);
			auto decoded_char = static_cast<char>(std::strtol(hex.c_str(), nullptr, 16));
			result += decoded_char;
			i += 2; // Move past the hex digits
		} else if (value[i] == '+') {
			result += ' '; // Convert '+' to space
		} else {
			result += value[i]; // Leave unchanged
		}
	}

	return result;
}

/**
 * Custom Request Handler
 */
class Handler : public oatpp::web::server::HttpRequestHandler {
public:
	std::shared_ptr<DuckDB> db;

	// Handler gets a duckdb instance
	Handler(std::shared_ptr<DuckDB> db) : db(db) {
	}
	/**
	 * Handle incoming request and return outgoing response.
	 */
	std::shared_ptr<OutgoingResponse> handle(const std::shared_ptr<IncomingRequest> &request) override {
		auto queryParamEncoded = request->getQueryParameter("query");

		if (!queryParamEncoded) {
			return ResponseFactory::createResponse(Status::CODE_400, "Query parameter 'query' is required");
		}

		auto queryParam = url_decode(queryParamEncoded);

		try {
			Connection con(*db->instance);
			auto result = con.Query(queryParam.c_str());

			if (!result->HasError()) {
				return ResponseFactory::createResponse(Status::CODE_200, result->ToString());
			} else {
				return ResponseFactory::createResponse(Status::CODE_500, result->GetError());
			}
		} catch (const std::exception& e) {
			return ResponseFactory::createResponse(Status::CODE_500, std::string("Error executing query: ") + e.what());
		}
	}
};

void run(std::shared_ptr<DuckDB> db) {

	/* Create Router for HTTP requests routing */
	auto router = oatpp::web::server::HttpRouter::createShared();

	/* Route GET - "/hello" requests to Handler */
	router->route("GET", "/hello", std::make_shared<Handler>(db));

	/* Create HTTP connection handler with router */
	auto connectionHandler = oatpp::web::server::HttpConnectionHandler::createShared(router);

	/* Create TCP connection provider */
	auto connectionProvider =
	    oatpp::network::tcp::server::ConnectionProvider::createShared({"localhost", 8000, oatpp::network::Address::IP_4}
	    );

	/* Create server which takes provided TCP connections and passes them to HTTP connection handler */
	oatpp::network::Server server(connectionProvider, connectionHandler);

	/* Priny info about server port */
	OATPP_LOGI("MyApp!!", "Server running on port %s", connectionProvider->getProperty("port").getData());

	/* Run server */
	server.run();
}

int start_server(std::shared_ptr<DuckDB> db) {

	/* Init oatpp Environment */
	oatpp::base::Environment::init();

	/* Run App */
	run(db);

	/* Destroy oatpp Environment */
	oatpp::base::Environment::destroy();

	return 0;
}
} // namespace duckdb