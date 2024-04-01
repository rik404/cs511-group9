// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess_proxy.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;

namespace ranger {

struct ActionHash {
 public:
  int operator()(const ActionPB& action) const {
    return action;
  }
};

struct RangerSubprocessMetrics : public subprocess::SubprocessMetrics {
  explicit RangerSubprocessMetrics(const scoped_refptr<MetricEntity>& entity);
};

typedef subprocess::SubprocessProxy<RangerRequestListPB, RangerResponseListPB,
                                    RangerSubprocessMetrics> RangerSubprocess;

// A client for the Ranger service that communicates with a Java subprocess.
//
// The Ranger subprocess itself is configured using xml files, like
// core-site.xml and ranger-kudu-security.xml. The only configuration that is
// coming from this class are environmental, like Java binary location, location
// of config files, and krb5.conf file (setting it doesn't enable Kerberos, that
// depends on core-site.xml).
//
// These methods return non-OK status only if something goes wrong between the
// client and the subprocess (e.g. IOError, EndOfFile, Corruption), but they
// never return NotAuthorized. The authz provider is responsible to return
// NotAuthorized based on RangerClient's out parameters and return Status.
class RangerClient {
 public:
  // The privilege scope can indicate the hierarchy of authorizables
  // (e.g. database -> table -> column). For example, authorizable 'db=a' has
  // database level scope, while authorizable 'db=a->table=b' has table level scope.
  // Note that COLUMN level scope is not defined in the enum as it is not
  // used yet in the code (although the concept still apply when
  // authorizing column level privileges).
  enum Scope {
    DATABASE,
    TABLE
  };

  // Creates a Ranger client.
  explicit RangerClient(Env* env, const scoped_refptr<MetricEntity>& metric_entity);

  // Starts the RangerClient, initializes the subprocess server.
  Status Start() WARN_UNUSED_RESULT;

  // Authorizes an action on the table. Sets 'authorized' to true if it's
  // authorized, false otherwise.
  Status AuthorizeAction(const std::string& user_name, const ActionPB& action,
                         const std::string& database, const std::string& table, bool is_owner,
                         bool requires_delegate_admin, bool* authorized,
                         Scope scope = Scope::TABLE) WARN_UNUSED_RESULT;

  // Authorizes action on multiple tables. It sets 'table_names' to the
  // tables the user is authorized to access.
  Status AuthorizeActionMultipleTables(const std::string& user_name, const ActionPB& action,
                                       std::unordered_map<std::string, bool>* tables)
    WARN_UNUSED_RESULT;

  // Authorizes action on multiple columns. It sets 'column_names' to the
  // columns the user is authorized to access.
  Status AuthorizeActionMultipleColumns(const std::string& user_name, const ActionPB& action,
                                        const std::string& database, const std::string& table,
                                        bool is_owner,
                                        std::unordered_set<std::string>* column_names)
    WARN_UNUSED_RESULT;

  // Authorizes multiple table-level actions on a single table. It sets
  // 'actions' to the actions the user is authorized to perform.
  Status AuthorizeActions(const std::string& user_name, const std::string& database,
                          const std::string& table, bool is_owner,
                          std::unordered_set<ActionPB, ActionHash>* actions,
                          Scope scope = Scope::TABLE) WARN_UNUSED_RESULT;

  // Refreshes policies in the Ranger subprocess. This does not invalidate the
  // existing cache and doesn't fail if Ranger service is unavailable, it simply
  // tries to refresh the policies from the server on a best effort basis.
  Status RefreshPolicies() WARN_UNUSED_RESULT;

  // Replaces the subprocess server in the subprocess proxy.
  void ReplaceServerForTests(std::unique_ptr<subprocess::SubprocessServer> server) {
    // Creates a dummy RangerSubprocess if it is not initialized.
    if (!subprocess_) {
      subprocess_.reset(new RangerSubprocess(env_, "", {""}, metric_entity_));
    }
    subprocess_->ReplaceServerForTests(std::move(server));
  }

 private:
  Env* env_;
  std::unique_ptr<RangerSubprocess> subprocess_;
  scoped_refptr<MetricEntity> metric_entity_;
};

// Validate Ranger configuration.
bool ValidateRangerConfiguration();

} // namespace ranger
} // namespace kudu
