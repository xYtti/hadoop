/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef LIBHDFSPP_OPTIONS_H_
#define LIBHDFSPP_OPTIONS_H_

#include "hdfspp/uri.h"

#include <algorithm>
#include <string>
#include <vector>
#include <map>

namespace hdfs {

class Token {
public:
    Token() {}

    Token (const Token &other) {
        setIdentifier(other.getIdentifier());
        setPassword(other.getPassword());
        setService(other.getService());
        setKind(other.getKind());
    }

    Token& operator= (const Token &other) {
        setIdentifier(other.getIdentifier());
        setPassword(other.getPassword());
        setService(other.getService());
        setKind(other.getKind());
        return *this;
    }

    const std::string & getIdentifier() const {
        return identifier;
    }

    void setIdentifier(const std::string & identifier_) {
        this->identifier = identifier_;
    }

    const std::string & getKind() const {
        return kind;
    }

    void setKind(const std::string & kind_) {
        this->kind = kind_;
    }

    const std::string & getPassword() const {
        return password;
    }

    void setPassword(const std::string & password_) {
        this->password = password_;
    }

    const std::string & getService() const {
        return service;
    }

    void setService(const std::string & service_) {
        this->service = service_;
    }

    bool hasValue() const {
        return !identifier.empty();
    }

private:
    std::string identifier;
    std::string password;
    std::string kind;
    std::string service;
};

struct NamenodeInfo {
  NamenodeInfo(const std::string &nameservice_, const std::string &nodename_, const URI &uri_) :
                nameservice(nameservice_), name(nodename_), uri(uri_) {}
  NamenodeInfo(){}
  //nameservice this belongs to
  std::string nameservice;
  //node name
  std::string name;
  //host:port
  URI uri;

  //get server hostname and port (aka service)
  std::string get_host() const;
  std::string get_port() const;
};

/**
 * Options to control the behavior of the libhdfspp library.
 **/
struct Options {
  /**
   * Time out of RPC requests in milliseconds.
   * Default: 30000
   **/
  int rpc_timeout;
  static const int kDefaultRpcTimeout = 30000;

  /**
   * Time to wait for an RPC connection before failing
   * Default: 30000
   **/
  int rpc_connect_timeout;
  static const int kDefaultRpcConnectTimeout = 30000;

  /**
   * Maximum number of retries for RPC operations
   **/
  int max_rpc_retries;
  static const int kNoRetry = 0;
  static const int kDefaultMaxRpcRetries = kNoRetry;

  /**
   * Number of ms to wait between retry of RPC operations
   **/
  int rpc_retry_delay_ms;
  static const int kDefaultRpcRetryDelayMs = 10000;

  /**
   * Exclusion time for failed datanodes in milliseconds.
   * Default: 60000
   **/
  unsigned int host_exclusion_duration;
  static const unsigned int kDefaultHostExclusionDuration = 600000;

  /**
   * URI to connect to if no host:port are specified in connect
   */
  URI defaultFS;

  /**
   * Namenodes used to provide HA for this cluster if applicable
   **/
  std::map<std::string, std::vector<NamenodeInfo>> services;


  /**
   * Client failover attempts before failover gives up
   **/
  int failover_max_retries;
  static const unsigned int kDefaultFailoverMaxRetries = 4;

  /**
   * Client failover attempts before failover gives up if server
   * connection is timing out.
   **/
  int failover_connection_max_retries;
  static const unsigned int kDefaultFailoverConnectionMaxRetries = 0;

  /*
   * Which form of authentication to use with the server
   * Default: simple
   */
  enum Authentication {
      kSimple,
      kKerberos
  };
  Authentication authentication;
  static const Authentication kDefaultAuthentication = kSimple;

  /**
   * Block size in bytes.
   * Default: 128 * 1024 * 1024 = 134217728
   **/
  long block_size;
  static const long kDefaultBlockSize = 128*1024*1024;

  /**
   * Asio worker thread count
   * default: -1, indicates number of hardware threads
   **/
  int io_threads_;
  static const int kDefaultIoThreads = -1;

  std::map<std::pair<std::string, std::string>, Token> tokens_;
  void addTokens(const Options &other) {
      const std::map<std::pair<std::string, std::string>, Token> tokens = other.getTokens();
      for (auto token : tokens)
      {
          this->addToken(token.second);
      }
  }

  void addToken(const Token &token) {
      std::string kind;
      std::string service;
      kind.resize(token.getKind().size());
      service.resize(token.getService().size());
      std::transform(token.getKind().begin(), token.getKind().end(), kind.begin(), tolower);
      std::transform(token.getService().begin(), token.getService().end(), service.begin(), tolower);

      if (kind != "hdfs_delegation_token") {
          return;
      }

      tokens_[std::make_pair(service, kind)] = token;
  }

  const std::map<std::pair<std::string, std::string>, Token> & getTokens() const {
      return tokens_;
  }

  Options();
};
}
#endif
