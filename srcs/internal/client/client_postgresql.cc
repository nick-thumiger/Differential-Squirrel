#include "client_postgresql.h"

#include <unistd.h>

#include <cstring>
#include <deque>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "absl/strings/str_format.h"
#include "client.h"
#include "libpq-fe.h"

using namespace std;
namespace {
PGconn *create_connection(std::string_view db_name) {

  std::string conninfo =
      absl::StrFormat("hostaddr=%s port=%s dbname=%s connect_timeout=4",
                      "127.0.0.1", 0, db_name);

  std::cerr << "Connection info: " << conninfo << std::endl;
  PGconn *result = PQconnectdb(conninfo.c_str());
  if (PQstatus(result) == CONNECTION_BAD) {
    fprintf(stderr, "Error1: %s\n", PQerrorMessage(result));
    std::cerr << "BAd" << std::endl;
  }
  return result;
}

void reset_database(PGconn *conn) {
  auto res = PQexec(conn, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
  PQclear(res);
}
};  // namespace

namespace client {

void PostgreSQLClient::initialize(YAML::Node config, const int database_number) {
  host_ = config["host"].as<std::string>();
  port_ = config["port"].as<std::string>();
  user_name_ = config["user_name"].as<std::string>();
  passwd_ = config["passwd"].as<std::string>();
  db_name_ = config["db_name"].as<std::string>();
  YAML::Node ports = config["ports"];
  if (database_number >= 0 && database_number < ports.size()) {
    port_ = ports[database_number].as<std::string>();
  } else {
    throw std::runtime_error("Invalid database_number for port selection in config");
  }
  std::cerr << "Sock path: " << sock_path_ << std::endl;
}

void PostgreSQLClient::prepare_env() {
  PGconn *conn = create_connection(db_name_);
  reset_database(conn);
  PQfinish(conn);
}

std::string PostgreSQLClient::get_startup_command() {
  // TODO: Function implementation
  return "";
}

std::string PostgreSQLClient::execute(const char *query, size_t size) {
  auto conn = create_connection(db_name_);

  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Error2: %s\n", PQerrorMessage(conn));
    PQfinish(conn);
    return "kServerCrash";
  }

  std::string cmd(query, size);

  auto res = PQexec(conn, cmd.c_str());
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Error3: %s\n", PQerrorMessage(conn));
    PQclear(res);
    return "kServerCrash";
  }

  if (PQresultStatus(res) != PGRES_COMMAND_OK &&
      PQresultStatus(res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "Error4: %s\n", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    return "kExecuteError";
  }
  PQclear(res);
  PQfinish(conn);
  return "kNormal";
}

void PostgreSQLClient::clean_up_env() {}

bool PostgreSQLClient::check_alive() {
  std::string conninfo = absl::StrFormat(
      "hostaddr=%s port=%d connect_timeout=4", "127.0.0.1", 5432);
  PGPing res = PQping(conninfo.c_str());
  return res == PQPING_OK;
}
}  // namespace client
