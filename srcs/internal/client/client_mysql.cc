#include "client_mysql.h"

#include <unistd.h>

#include <cstring>
#include <deque>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "mysql.h"
#include "mysqld_error.h"

using namespace std;
namespace {
bool is_crash_response(int response) {
  return response == CR_SERVER_LOST || response == CR_SERVER_GONE_ERROR;
}
};  // namespace

namespace client {

void MySQLClient::initialize(YAML::Node config, int database_number) {
  host_ = config["host"].as<std::string>();
  user_name_ = config["user_name"].as<std::string>();
  passwd_ = config["passwd"].as<std::string>();
  db_prefix_ = config["db_prefix"].as<std::string>();
  auto get_from_config = [&config](const std::string &field, int index) -> std::string {
    if (config[field] && config[field].IsSequence() && index >= 0 && index < config[field].size()) {
      return config[field][index].as<std::string>();
    } else {
      throw std::out_of_range("Invalid index or missing field '" + field + "' in the configuration");
    }
  };

  port_ = get_from_config("ports", database_number);
  executable = get_from_config("executables", database_number);
  basedir = get_from_config("basedirs", database_number);
  datadir = get_from_config("datadirs", database_number);
  pid_file = get_from_config("pid_files", database_number);
  sock_path_ = get_from_config("sock_paths", database_number);
  extra_running_parameters = config["startup_cmd"].as<std::string>();
  startup_command = executable + " --socket=" + sock_path_ + " --pid_file=" + pid_file + " --port=" + port_ + " --basedir=" + basedir + " --datadir=" + datadir + extra_running_parameters;
}

std::string MySQLClient::get_startup_command() {
  return startup_command;
}


void MySQLClient::prepare_env() {
  ++database_id_;
  std::string database_name = db_prefix_ + std::to_string(database_id_);
  if (!create_database(database_name)) {
    std::cerr << "Failed to create database." << std::endl;
  }
}

std::string MySQLClient::execute(const char *query, size_t size) {
  // Create a connection for executing the query
  // Check the response.
  // Return status accordingly.
  std::string database_name = db_prefix_ + std::to_string(database_id_);
  std::optional<MYSQL> connection = create_connection(database_name);
  if (!connection.has_value()) {
    std::cerr << "Cannot creat connection at execute " << std::endl;
    return "kServerCrash";
  }

  int server_response = mysql_real_query(&(*connection), query, size);
  if (is_crash_response(server_response)) {
    std::cerr << "Cannot mySQL_QUERY " << std::endl;
    return "kServerCrash";
  }
  std::string rows;

  bool more;
  bool first = true;
  do {
    int status;
    if (!first)
      status = mysql_next_result(&(*connection));
    first = false;

    int affected_rows = mysql_affected_rows(&(*connection));
    MYSQL_RES *result = mysql_store_result(&(*connection));
    
    rows += std::to_string(affected_rows) + " ";
    if (result) {  // a SELECT query, rows were returned
      MYSQL_ROW row;
      while ((row = mysql_fetch_row(result))) {
        unsigned long *lengths = mysql_fetch_lengths(result);
        for (unsigned int i = 0; i < mysql_num_fields(result); i++) {
          rows += std::string(row[i], lengths[i]);
        }
      }
    }


    mysql_free_result(result);
    
    more = mysql_more_results(&(*connection));
  } while (more);
  ExecutionStatus server_status = clean_up_connection(*connection);
  mysql_close(&(*connection));
  return rows;
}

void MySQLClient::clean_up_env() {
  std::string database_name = db_prefix_ + std::to_string(database_id_);
  string reset_query = "DROP DATABASE IF EXISTS " + database_name + ";";
  std::optional<MYSQL> connection = create_connection("");
  if (!connection.has_value()) {
    return;
  }
  // TODO: clean up the connection.
  mysql_real_query(&(*connection), reset_query.c_str(), reset_query.size());
  clean_up_connection((*connection));
  mysql_close(&(*connection));
}

std::optional<MYSQL> MySQLClient::create_connection(std::string_view db_name) {
  MYSQL result;
  if (mysql_init(&result) == NULL) return std::nullopt;

  if (mysql_real_connect(&result, host_.c_str(), user_name_.c_str(),
                         passwd_.c_str(), db_name.data(), std::stoi(port_), sock_path_.c_str(),
                         CLIENT_MULTI_STATEMENTS) == NULL) {
    std::cerr << "Create connection failed: " << mysql_errno(&result)
              << mysql_error(&result) << std::endl;
    mysql_close(&result);
    return std::nullopt;
  }

  return result;
}

bool MySQLClient::check_alive() {
  std::optional<MYSQL> connection = create_connection("");
  if (!connection.has_value()) {
    return false;
  }
  mysql_close(&(*connection));
  return true;
}

bool MySQLClient::create_database(const std::string &database) {
  MYSQL tmp_m;

  if (mysql_init(&tmp_m) == NULL) {
    mysql_close(&tmp_m);
    return false;
  }

  if (mysql_real_connect(&tmp_m, host_.c_str(), user_name_.c_str(),
                         passwd_.c_str(), nullptr, 0, sock_path_.c_str(),
                         CLIENT_MULTI_STATEMENTS) == NULL) {
    fprintf(stderr, "Connection error3 %d, %s\n", mysql_errno(&tmp_m),
            mysql_error(&tmp_m));
    mysql_close(&tmp_m);
    return false;
  }

  string cmd = "CREATE DATABASE IF NOT EXISTS " + database + ";";
  // TODO: Check server response status.
  mysql_real_query(&tmp_m, cmd.c_str(), cmd.size());
  // std::cerr << "Server response: " << server_response << std::endl;
  clean_up_connection(tmp_m);
  mysql_close(&tmp_m);
  return true;
}

ExecutionStatus MySQLClient::clean_up_connection(MYSQL &mm) {
  int res = -1;
  do {
    auto q_result = mysql_store_result(&mm);
    if (q_result) mysql_free_result(q_result);
  } while ((res = mysql_next_result(&mm)) == 0);

  if (res != -1) {
    res = mysql_errno(&mm);
    if (is_crash_response(res)) {
      // std::cerr << "Found a crash!" << std::endl;
      return kServerCrash;
    }
    if (res == ER_PARSE_ERROR) {
      // std::cerr << "Syntax error" << std::endl;
      return kSyntaxError;
    } else {
      // std::cerr << "Semantic error" << std::endl;
      return kSemanticError;
    }
  }
  // std::cerr << "Normal" << std::endl;
  return kNormal;
}
};  // namespace client
