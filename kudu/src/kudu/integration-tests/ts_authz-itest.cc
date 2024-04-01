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

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/data_gen_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h" // IWYU pragma: keep
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/barrier.h"
#include "kudu/util/bitset.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::sp::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduInsert;
using kudu::client::KuduError;
using kudu::client::KuduUpdate;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTransaction;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::master::RefreshAuthzCacheRequestPB;
using kudu::master::RefreshAuthzCacheResponsePB;
using kudu::ranger::ActionPB;
using kudu::ranger::AuthorizationPolicy;
using kudu::ranger::PolicyItem;
using kudu::ranger::MiniRanger;
using kudu::rpc::RpcController;
using kudu::tablet::WritePrivileges;
using kudu::tablet::WritePrivilegeType;
using kudu::tools::RunKuduTool;
using std::pair;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

// Encapsulates the set of read and write privileges granted to a user. This is
// used for easier composability of tests.
//
// Note: while full table scan privileges could also be included, leaving this
// out simplifies the below tests, which are aimed at testing functionality of
// privileges granted by authz tokens end-to-end; privilege-checking for
// different actions is tested in more depth elsewhere.
struct RWPrivileges {
  // The set of write privileges a user may be granted for a table.
  WritePrivileges table_write_privileges;

  // The set of column names that the user is authorized to scan.
  unordered_set<string> column_scan_privileges;
};

const WritePrivileges kFullPrivileges = {
  WritePrivilegeType::INSERT,
  WritePrivilegeType::UPDATE,
  WritePrivilegeType::DELETE,
};

// Returns a randomly generated set of read and write privileges, ensuring that
// it contains at least one read and one write privilege.
RWPrivileges GeneratePrivileges(const unordered_set<string>& all_cols, ThreadSafeRandom* prng) {
  WritePrivilegeType write_privilege =
      SelectRandomElement<WritePrivileges, WritePrivilegeType, ThreadSafeRandom>(
      kFullPrivileges, prng);
  vector<string> scan_privileges =
      SelectRandomSubset<unordered_set<string>, string, ThreadSafeRandom>(
      all_cols, /*min_to_return*/1, prng);
  RWPrivileges privileges;
  privileges.table_write_privileges = WritePrivileges({ write_privilege });
  privileges.column_scan_privileges =
      unordered_set<string>(scan_privileges.begin(), scan_privileges.end());
  return privileges;
}

// Returns the complentary set of privileges to 'orig_privileges'. This is
// useful for generating operations that should fail, if a user is granted the
// privileges in 'orig_privileges'.
RWPrivileges ComplementaryPrivileges(const unordered_set<string>& all_cols,
                                     const RWPrivileges& orig_privileges) {
  RWPrivileges privileges;
  for (const auto& wp : kFullPrivileges) {
    if (!ContainsKey(orig_privileges.table_write_privileges, wp)) {
      InsertOrDie(&privileges.table_write_privileges, wp);
    }
  }
  for (const auto& col : all_cols) {
    if (!ContainsKey(orig_privileges.column_scan_privileges, col)) {
      InsertOrDie(&privileges.column_scan_privileges, col);
    }
  }
  return privileges;
}

// Performs a write operation to 'table' that should be allowed based on the
// privileges in 'write_privileges', using 'prng' to determine the operation.
Status PerformWrite(const WritePrivileges& write_privileges,
                    ThreadSafeRandom* prng,
                    KuduTable* table,
                    KuduSession* session = nullptr) {
  WritePrivilegeType op_type =
      SelectRandomElement<WritePrivileges, WritePrivilegeType, ThreadSafeRandom>(
      write_privileges, prng);
  shared_ptr<KuduSession> new_session;
  if (!session) {
    new_session = table->client()->NewSession();
    session = new_session.get();
  }
  CHECK(session);
  const auto unwrap_session_error = [session] (Status s) {
    if (s.IsIOError()) {
      vector<KuduError*> errors;
      session->GetPendingErrors(&errors, nullptr);
      ElementDeleter deleter(&errors);
      CHECK_EQ(1, errors.size());
      return errors[0]->status();
    }
    return s;
  };
  // Note: we could test UPSERTs, but it complicates the logic, and UPSERTs are
  // tested elsewhere anyway.
  switch (op_type) {
    case WritePrivilegeType::INSERT: {
        unique_ptr<KuduInsert> ins(table->NewInsert());
        GenerateDataForRow(table->schema(), prng->Next32(), prng, ins->mutable_row());
        return unwrap_session_error(session->Apply(ins.release()));
      }
      break;
    case WritePrivilegeType::UPDATE: {
        unique_ptr<KuduUpdate> upd(table->NewUpdate());
        GenerateDataForRow(table->schema(), prng->Next32(), prng, upd->mutable_row());
        return unwrap_session_error(session->Apply(upd.release()));
      }
      break;
    case WritePrivilegeType::DELETE: {
        unique_ptr<KuduDelete> del(table->NewDelete());
        KuduPartialRow* row = del->mutable_row();
        RETURN_NOT_OK(row->SetInt32(0, prng->Next32()));
        return unwrap_session_error(session->Apply(del.release()));
      }
      break;
  }
  return Status::OK();
}

// Performs a scan operation to 'table' that should be allowed if the user is
// granted scan privileges on all columns in 'columns'. If provided, uses
// 'prng' to select a subset of rows to scan; otherwise uses all columns.
Status PerformScan(const unordered_set<string>& columns,
                   ThreadSafeRandom* prng,
                   KuduTable* table) {
  vector<string> cols_to_scan = prng
      ? SelectRandomSubset<unordered_set<string>, string, ThreadSafeRandom>(
            columns, /*min_to_return*/1, prng)
      : vector<string>(columns.begin(), columns.end());
  KuduScanner scanner(table);
  RETURN_NOT_OK(scanner.SetTimeoutMillis(30000));
  RETURN_NOT_OK(scanner.SetProjectedColumnNames(cols_to_scan));
  RETURN_NOT_OK(scanner.Open());
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK(scanner.NextBatch(&batch));
  }
  return Status::OK();
}

// Performs an action that should be allowed with the given set of
// privileges.
Status PerformAction(const RWPrivileges& privileges,
                     ThreadSafeRandom* prng, KuduTable* table) {
  bool can_write = !privileges.table_write_privileges.empty();
  bool can_scan = !privileges.column_scan_privileges.empty();
  CHECK(can_write || can_scan);
  // If the user can scan and write, flip a coin for what to do. Otherwise,
  // just perform whichever it can.
  bool should_write = (can_write && can_scan && rand() % 2 == 0) ||
                      (can_write && !can_scan);
  if (should_write) {
    CHECK(can_write);
    RETURN_NOT_OK(PerformWrite(privileges.table_write_privileges, prng, table));
  } else {
    CHECK(can_scan);
    RETURN_NOT_OK(PerformScan(privileges.column_scan_privileges, prng, table));
  }
  return Status::OK();
}

constexpr int kNumUsers = 3;
constexpr const char* kAdminUser = "test-admin";

constexpr int kNumTables = 3;
constexpr int kNumColsPerTable = 3;
constexpr const char* kDb = "db";
constexpr const char* kTablePrefix = "table";

constexpr int kAuthzTokenTTLSecs = 1;
constexpr int kAuthzCacheTTLMultiplier = 3;

} // anonymous namespace

// Test harness used to set up a cluster and grant privileges, that is agnostic
// the underlying authorization service (e.g. Ranger).
class TSAuthzITestHarness {
 public:
  virtual ~TSAuthzITestHarness() = default;

  ExternalMiniClusterOptions GetClusterOpts() const {
    ExternalMiniClusterOptions opts;
    opts.enable_kerberos = true;
    // Set a low token timeout so we can ensure retries are working properly.
    opts.extra_master_flags.emplace_back(Substitute("--authz_token_validity_seconds=$0",
                                                    kAuthzTokenTTLSecs));
    // In addition to our users, we will be using the "kudu" user to perform
    // administrative tasks like creating tables.
    opts.extra_master_flags.emplace_back(
        Substitute("--user_acl=kudu,$0", JoinStrings(users_, ",")));
    opts.extra_tserver_flags.emplace_back(
        Substitute("--user_acl=$0", JoinStrings(users_, ",")));
    opts.extra_tserver_flags.emplace_back("--tserver_enforce_access_control=true");

    return opts;
  }

  // Sets up the external services (e.g. Ranger).
  virtual void SetUpExternalMiniServiceOpts(ExternalMiniClusterOptions* opts) = 0;
  virtual Status SetUpExternalServiceClients(const unique_ptr<ExternalMiniCluster>& cluster) = 0;

  // Sets up roles and privileges so admins have full privileges to the
  // database. Also set up any user mappings necessary.
  virtual Status SetUpCredentials() = 0;

  // Sets up table-related metadata (e.g. HMS database, columns).
  virtual Status SetUpTables() = 0;

  // Grants privileges on the given table/column.
  // These abide the hierarchical definition of privileges, so if granting privileges on a
  // table, these grant privileges on the table and all columns in that table.
  virtual Status GrantTablePrivilege(const string& db, const string& tbl, const string& user,
                                     const string& action, bool admin,
                                     const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantColumnPrivilege(const string& db, const string& tbl, const string& col,
                                      const string& user, const string& action, bool admin,
                                      const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status RefreshAuthzPolicies(const unique_ptr<ExternalMiniCluster>& cluster) = 0;

  // Creates a table named 'table_ident' with 'kNumColsPerTable' columns.
  Status CreateTable(const string& table_ident,
                     const shared_ptr<KuduClient>& client,
                     const string& owner) {
    KuduSchema schema;
    KuduSchemaBuilder b;
    auto iter = cols_.begin();
    b.AddColumn(*iter++)->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    while (iter < cols_.end()) {
      b.AddColumn(*iter++)->Type(KuduColumnSchema::INT32);
    }
    RETURN_NOT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    return table_creator->table_name(table_ident)
        .schema(&schema)
        .set_range_partition_columns({"col0"})
        .num_replicas(1)
        .set_owner(owner)
        .Create();
  }

  void AddUsers() {
    for (int u = 0; u < kNumUsers; u++) {
      users_.emplace_back(Substitute("user$0", u));
    }
  }

  // A list of users that may try to do things.
  vector<string> users_;

  // A list of columns that each table should have.
  vector<string> cols_;
};

namespace {
ActionPB StringToActionPB(string s) {
  ToLowerCase(&s);
  if (s == "select") return ActionPB::SELECT;
  if (s == "insert") return ActionPB::INSERT;
  if (s == "update") return ActionPB::UPDATE;
  if (s == "delete") return ActionPB::DELETE;
  if (s == "alter") return ActionPB::ALTER;
  if (s == "create") return ActionPB::CREATE;
  if (s == "drop") return ActionPB::DROP;
  if (s == "all") return ActionPB::ALL;
  if (s == "metadata") return ActionPB::METADATA;
  LOG(FATAL) << "unknown ActionPB string";
}
} // anonymous namespace

class RangerITestHarness : public TSAuthzITestHarness {
 public:
  static constexpr int kSleepAfterNewPolicyMs = 1200;
  void SetUpExternalMiniServiceOpts(ExternalMiniClusterOptions* opts) override {
    opts->enable_ranger = true;
  }
  Status SetUpExternalServiceClients(const unique_ptr<ExternalMiniCluster>& cluster) override {
    ranger_ = cluster->ranger();
    return Status::OK();
  }
  Status SetUpCredentials() override {
    AuthorizationPolicy policy;
    policy.databases = { kDb };
    policy.tables = { "*" };
    policy.columns = { "*" };
    policy.items.emplace_back(PolicyItem({ kAdminUser }, { ActionPB::ALL }, true));
    return ranger_->AddPolicy(std::move(policy));
  }
  Status SetUpTables() override {
    // Finally populate a set of column names to use for our tables.
    for (int i = 0; i < kNumColsPerTable; i++) {
      cols_.emplace_back(Substitute("col$0", i));
    }
    return Status::OK();
  }

  Status RefreshAuthzPolicies(const unique_ptr<ExternalMiniCluster>& cluster) override {
    RefreshAuthzCacheRequestPB req;
    RefreshAuthzCacheResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(cluster->master_proxy()->RefreshAuthzCache(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  Status GrantTablePrivilege(const string& db, const string& tbl, const string& user,
                             const string& action, bool admin,
                             const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases = { db };
    policy.tables = { tbl };
    policy.columns = { "*" };
    policy.items.emplace_back(PolicyItem({ user }, { StringToActionPB(action) }, admin));
    RETURN_NOT_OK(ranger_->AddPolicy(std::move(policy)));
    return RefreshAuthzPolicies(cluster);
  }
  Status GrantColumnPrivilege(const string& db, const string& tbl, const string& col,
                              const string& user, const string& action, bool admin,
                              const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases = { db };
    policy.tables = { tbl };
    policy.columns = { col };
    policy.items.emplace_back(PolicyItem({ user }, { StringToActionPB(action) }, admin));
    RETURN_NOT_OK(ranger_->AddPolicy(std::move(policy)));
    return RefreshAuthzPolicies(cluster);
  }
 private:
  MiniRanger* ranger_;
};

// TODO(awong): refactor so we can share code between master_authz-itest.
enum HarnessEnum {
  kRanger,
};
string HarnessEnumToString(HarnessEnum h) {
  switch (h) {
    case kRanger:
      return "Ranger";
  }
  return "";
}

// These tests will use the HMS and an Authz provider, and thus, are very slow.
// SKIP_IF_SLOW_NOT_ALLOWED() should be the very first thing called in the body
// of every test based on this test class.
class TSAuthzITest : public ExternalMiniClusterITestBase,
                     public ::testing::WithParamInterface<HarnessEnum> {
 public:
  void SetUp() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    switch (GetParam()) {
      case kRanger:
        harness_.reset(new RangerITestHarness());
        break;
      default:
        LOG(FATAL) << "unknown harness";
    }
    ExternalMiniClusterITestBase::SetUp();
    harness_->AddUsers();
    ExternalMiniClusterOptions opts = GetClusterOpts();
    harness_->SetUpExternalMiniServiceOpts(&opts);
    NO_FATALS(StartClusterWithOpts(std::move(opts)));

    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal("kudu"));
    ASSERT_OK(cluster_->kdc()->Kinit("kudu"));

    ASSERT_OK(harness_->SetUpExternalServiceClients(cluster_));
    ASSERT_OK(harness_->SetUpCredentials());
    ASSERT_OK(harness_->RefreshAuthzPolicies(cluster_));
    ASSERT_OK(harness_->SetUpTables());

    // Create a client as the admin user, who now has admin privileges on the
    // database.
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(kAdminUser));
    ASSERT_OK(cluster_->kdc()->Kinit(kAdminUser));
    ASSERT_OK(cluster_->CreateClient(/*builder*/nullptr, &client_));
  }

  Status CreateTable(const string& table_ident, const string& owner) {
    return harness_->CreateTable(table_ident, client_, owner);
  }

  void TearDown() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    ExternalMiniClusterITestBase::TearDown();
  }

 protected:
  virtual ExternalMiniClusterOptions GetClusterOpts() const {
    return harness_->GetClusterOpts();
  }

  unique_ptr<TSAuthzITestHarness> harness_;
};

// Tests authorizing read and write operations coming from multiple concurrent
// users for multiple tables.
TEST_P(TSAuthzITest, TestReadsAndWrites) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // First, set up the tables.
  vector<string> tables;
  for (int i = 0; i < kNumTables; i++) {
    string table_name = Substitute("$0$1", kTablePrefix, i);
    ASSERT_OK(CreateTable(Substitute("$0.$1", kDb, table_name), harness_->users_[0]));
    tables.emplace_back(std::move(table_name));
  }

  // Keep track of the privileges that each user has been granted and not been
  // granted per table.
  typedef pair<RWPrivileges, RWPrivileges> GrantedNotGrantedPrivileges;
  typedef unordered_map<string, GrantedNotGrantedPrivileges> TableNameToPrivileges;
  unordered_map<string, TableNameToPrivileges> user_to_privileges;

  // Set up a bunch of clients for each user.
  unordered_map<string, vector<shared_ptr<KuduClient>>> user_to_clients;
  ThreadSafeRandom prng(SeedRandom());
  unordered_set<string> cols(harness_->cols_.begin(), harness_->cols_.end());
  static constexpr int kNumClientsPerUser = 4;
  for (int i = 0; i < kNumUsers; i++) {
    const string& user = harness_->users_[i];
    // Register the user with the KDC.
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
    ASSERT_OK(cluster_->kdc()->Kinit(user));

    // Set up multiple clients for each user.
    vector<shared_ptr<KuduClient>> clients;
    for (int i = 0; i < kNumClientsPerUser; i++) {
      shared_ptr<KuduClient> client;
      ASSERT_OK(cluster_->CreateClient(nullptr, &client));
      clients.emplace_back(std::move(client));
    }
    EmplaceOrDie(&user_to_clients, user, std::move(clients));
    const string& user_to_grant = i ? user : "{OWNER}";

    // Generate privileges for each user for every table, and grant
    // the appropriate privileges.
    TableNameToPrivileges table_to_privileges;
    for (const string& table_name : tables) {
      RWPrivileges granted_privileges = GeneratePrivileges(cols, &prng);
      for (const auto& wp : granted_privileges.table_write_privileges) {
        ASSERT_OK(harness_->GrantTablePrivilege(kDb, table_name, user_to_grant,
                                                WritePrivilegeToString(wp),
                                                /*admin=*/false, cluster_));
      }
      for (const auto& col : granted_privileges.column_scan_privileges) {
        ASSERT_OK(harness_->GrantColumnPrivilege(kDb, table_name, col,
                                                 user_to_grant, "SELECT",
                                                 /*admin=*/false, cluster_));
      }
      RWPrivileges not_granted_privileges = ComplementaryPrivileges(cols, granted_privileges);
      InsertOrDie(&table_to_privileges, table_name,
          { std::move(granted_privileges), std::move(not_granted_privileges) });
    }
    EmplaceOrDie(&user_to_privileges, user, std::move(table_to_privileges));
  }

  // In parallel, have each user's clients perform a series of operations on a
  // table for some extended period of time (longer than the token timeout). Do
  // this for a few tables for each client.
  static constexpr int kNumOpPeriods = 3;
  static const MonoDelta kPeriodTime = MonoDelta::FromSeconds(kAuthzTokenTTLSecs * 3);
  vector<thread> threads;
  Barrier b(kNumUsers * kNumClientsPerUser);
  SCOPED_CLEANUP({
    for (auto& t : threads) {
      t.join();
    }
  });
  for (const string& user : harness_->users_) {
    // Start a thread for every user that performs a bunch of operations.
    const auto* const table_to_privileges = FindOrNull(user_to_privileges, user);
    for (const auto& client_sp : FindOrDie(user_to_clients, user)) {
      KuduClient* client = client_sp.get();
      threads.emplace_back([client, table_to_privileges, &b, &tables, &prng] {
        b.Wait();
        // Perform a bunch of operations, switching back and forth between
        // different tables to ensure a client uses the appropriate privileges.
        for (int i = 0; i < kNumOpPeriods; i++) {
          const auto& table_name =
              SelectRandomElement<vector<string>, string, ThreadSafeRandom>(tables, &prng);
          shared_ptr<KuduTable> table;
          ASSERT_OK(client->OpenTable(Substitute("$0.$1", kDb, table_name), &table));
          const MonoTime end_time = MonoTime::Now() + kPeriodTime;
          while (MonoTime::Now() < end_time) {
            const auto& privileges = FindOrDie(*table_to_privileges, table_name);
            const auto& granted_privileges = privileges.first;
            const auto& non_granted_privileges = privileges.second;
            // Perform a permitted operation. We might not get an OK status if
            // e.g. we're inserting a row that already exists, but the operation
            // should always be permitted.
            Status s = PerformAction(granted_privileges, &prng, table.get());
            ASSERT_FALSE(s.IsNotAuthorized()) << s.ToString();
            ASSERT_STR_NOT_CONTAINS(s.ToString(), "not authorized");

            // Now perform an operation based on the privileges we _don't_ have;
            // this should always yield authorization errors.
            s = PerformAction(non_granted_privileges, &prng, table.get());
            ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
            ASSERT_STR_CONTAINS(s.ToString(), "not authorized");
          }
        }
      });
    }
  }
}

// Test for a couple of scenarios related to alter tables.
TEST_P(TSAuthzITest, TestAlters) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  static const string kTableName = "table";
  const string table_ident = Substitute("$0.$1", kDb, kTableName);
  const string user = "user0";
  ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
  ASSERT_OK(cluster_->kdc()->Kinit(user));

  ASSERT_OK(CreateTable(table_ident, user));

  shared_ptr<KuduClient> user_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &user_client));

  // Note: we only need privileges on the metadata for OpenTable() calls.
  // METADATA isn't a first-class privilege and won't get carried over
  // on table rename, so we just grant INSERT privileges.
  ASSERT_OK(harness_->GrantTablePrivilege(kDb, kTableName, user, "INSERT", /*admin=*/false,
                                          cluster_));

  // First, grant privileges on a new column that doesn't yet exist. Once that
  // column is created, we should be able to scan it immediately.
  const string new_column = Substitute("col$0", kNumColsPerTable);
  ASSERT_OK(harness_->GrantColumnPrivilege(kDb, kTableName, new_column, user, "SELECT",
                                           /*admin=*/false, cluster_));
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_ident));
    table_alterer->AddColumn(new_column)->Type(KuduColumnSchema::INT32);
    ASSERT_OK(table_alterer->Alter());
  }
  shared_ptr<KuduTable> table;
  ASSERT_OK(user_client->OpenTable(table_ident, &table));
  ASSERT_OK(PerformScan({ new_column }, /*prng=*/nullptr, table.get()));

  // Now create another column and grant the user privileges for that column.
  const string another_column = Substitute("col$0", kNumColsPerTable + 1);
  ASSERT_OK(harness_->GrantColumnPrivilege(kDb, kTableName, another_column, user, "SELECT",
                                           /*admin=*/false, cluster_));
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_ident));
    table_alterer->AddColumn(another_column)->Type(KuduColumnSchema::INT32);
    ASSERT_OK(table_alterer->Alter());
  }

  ASSERT_OK(user_client->OpenTable(table_ident, &table));

  // Wait the full duration of the cache TTL, and an additional full token TTL.
  // This ensures that the client's token will expire we will get a new one
  // with the most up-to-date privileges from the authorization provider.
  SleepFor(MonoDelta::FromSeconds(kAuthzTokenTTLSecs * (1 + kAuthzCacheTTLMultiplier)));
  ASSERT_OK(PerformScan({ another_column }, /*prng=*/nullptr, table.get()));

  // Now rename the table to something else.
  // We need to explicitly grant privileges on the new table name.
  const string kNewTableName = "newtable";
  const string new_table_ident = Substitute("$0.$1", kDb, kNewTableName);
  ASSERT_OK(harness_->GrantTablePrivilege(kDb, kNewTableName, user, "SELECT", /*admin=*/false,
                                          cluster_));
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_ident));
    table_alterer->RenameTo(new_table_ident);
    ASSERT_OK(table_alterer->Alter());
  }
  ASSERT_OK(user_client->OpenTable(new_table_ident, &table));
  ASSERT_OK(PerformScan({ another_column }, nullptr, table.get()));
}

TEST_P(TSAuthzITest, TableScanCLI) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  static const string kTableName = "table";
  const string table_ident = Substitute("$0.$1", kDb, kTableName);
  const string user = "user0";
  ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
  ASSERT_OK(cluster_->kdc()->Kinit(user));

  ASSERT_OK(CreateTable(table_ident, user));

  // Note: we only need privileges on the metadata for OpenTable() calls.
  ASSERT_OK(harness_->GrantTablePrivilege(
      kDb, kTableName, user, "METADATA", /*admin=*/false, cluster_));
  ASSERT_OK(harness_->GrantColumnPrivilege(
      kDb, kTableName, "col0", user, "SELECT", /*admin=*/false, cluster_));

  vector<string> master_addrs;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    master_addrs.emplace_back(hp.ToString());
  }
  const auto master_addrs_str = JoinStrings(master_addrs, ",");

  // Should successfully scan data in column 'col0' since granted privileged
  // explicitly.
  {
    string out;
    string err;
    const vector<string> args{ "table",
                               "scan",
                               master_addrs_str,
                               table_ident,
                               "--columns=col0"
                             };
    const auto s = RunKuduTool(args, &out, &err);
    ASSERT_TRUE(s.ok()) << s.ToString() << " stderr: " << err;
  }

  // Should not be able to scan data in other columns than column 'col0' since
  // no privilege has been granted on those.
  for (const auto& columns : { "col1", "col0,col1", "col1,col0",
                               "col2", "col0,col2", "col2,col0",
                               "col1,col2", "col0,col1,col2", "*" }) {
    string out;
    string err;
    const vector<string> args{ "table",
                               "scan",
                               master_addrs_str,
                               table_ident,
                               Substitute("--columns=$0", columns),
                             };
    const auto s = RunKuduTool(args, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString() << " stderr: " << err;
    ASSERT_STR_CONTAINS(s.ToString(), "process exited with non-zero status");
    ASSERT_STR_CONTAINS(err, "Not authorized: not authorized to Scan");
  }

  // Should not be able to scan data if trying to access all the columns.
  {
    string out;
    string err;
    const vector<string> args{ "table",
                               "scan",
                               master_addrs_str,
                               table_ident,
                             };
    const auto s = RunKuduTool(args, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString() << " stderr: " << err;
    ASSERT_STR_CONTAINS(s.ToString(), "process exited with non-zero status");
    ASSERT_STR_CONTAINS(err, "Not authorized: not authorized to Scan");
  }
}

INSTANTIATE_TEST_SUITE_P(AuthzProviders, TSAuthzITest,
    ::testing::Values(kRanger),
    [] (const testing::TestParamInfo<TSAuthzITest::ParamType>& info) {
      return HarnessEnumToString(info.param);
    });


class TSAuthzTxnOpsITest : public TSAuthzITest {
 protected:
  static Status CountRows(KuduTable* table, int64_t* count) {
    KuduScanner scanner(table);
    RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
    RETURN_NOT_OK(scanner.SetProjectedColumnNames({}));
    RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_YOUR_WRITES));
    RETURN_NOT_OK(scanner.Open());

    int64_t num_rows = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      RETURN_NOT_OK(scanner.NextBatch(&batch));
      num_rows += batch.NumRows();
    }
    *count = num_rows;
    return Status::OK();
  }

  ExternalMiniClusterOptions GetClusterOpts() const override {
    auto opts = TSAuthzITest::GetClusterOpts();
    // Since txn-related functionality is disabled for a while, a couple of
    // flags should be overriden to allow running tests scenarios in txn-enabled
    // environment. Also, since the replication factor of the txn status table
    // isn't crucial for this test, set its RF=1 to work with a single tablet
    // server in the test cluster.
    opts.extra_master_flags.emplace_back("--txn_manager_enabled");
    opts.extra_master_flags.emplace_back("--txn_manager_status_table_num_replicas=1");
    opts.extra_tserver_flags.emplace_back("--enable_txn_system_client_init");
    opts.num_tablet_servers = 3;

    return opts;
  }
};

// Make sure users can perform basic txn-related operations on 'empty' multi-row
// transactions.
TEST_P(TSAuthzTxnOpsITest, BasicOpsOnEmptyTransactions) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const string user = "user0";
  ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
  ASSERT_OK(cluster_->kdc()->Kinit(user));
  ASSERT_OK(CreateTable(Substitute("$0.table", kDb), user));

  shared_ptr<KuduClient> user_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &user_client));

  // It should be possible to start and commit an empty transaction.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(user_client->NewTransaction(&txn));
    ASSERT_OK(txn->Commit());
    Status txn_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &txn_status));
    ASSERT_TRUE(is_complete);
    ASSERT_OK(txn_status);
  }

  // It should be possible to start and commit an empty transaction in an
  // asynchronous way.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(user_client->NewTransaction(&txn));
    ASSERT_OK(txn->StartCommit());
    ASSERT_EVENTUALLY([&] {
      Status txn_status;
      bool is_complete = false;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &txn_status));
      ASSERT_TRUE(is_complete);
      ASSERT_OK(txn_status);
    });
  }

  // It should be possible to start and rollback an empty transaction.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(user_client->NewTransaction(&txn));
    ASSERT_OK(txn->Rollback());
    ASSERT_EVENTUALLY([&] {
      Status txn_status;
      bool is_complete = true;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &txn_status));
      ASSERT_TRUE(is_complete);
      ASSERT_TRUE(txn_status.IsAborted()) << txn_status.ToString();
    });
  }
}

// Make sure authorized users can successfully perform txn-related operations
// while inserting rows into tables. In this scenario, the user is automatically
// granted the 'METADATA' privilege on a test table by granting the 'INSERT'
// privilege, so KuduTable::Open() succeeds.
TEST_P(TSAuthzTxnOpsITest, BasicOperationsForAuthorizedUsers) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const string user = "user0";
  ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
  ASSERT_OK(cluster_->kdc()->Kinit(user));

  shared_ptr<KuduClient> user_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &user_client));
  ThreadSafeRandom prng(SeedRandom());

  // Start and then commit a transaction with a single row inserted.
  {
    static const string kTableName = "table_commit";
    const string table_name = Substitute("$0.$1", kDb, kTableName);
    ASSERT_OK(CreateTable(table_name, user));

    // Grant 'INSERT' privilege, so it's possible to insert a few rows.
    ASSERT_OK(harness_->GrantTablePrivilege(
        kDb, kTableName, user, "INSERT", /*admin=*/false, cluster_));
    // Grant 'SELECT' to allow for counting rows in the table.
    ASSERT_OK(harness_->GrantTablePrivilege(
        kDb, kTableName, user, "SELECT", /*admin=*/false, cluster_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(user_client->OpenTable(table_name, &table));

    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(user_client->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::FlushMode::MANUAL_FLUSH));
    ASSERT_OK(PerformWrite(
        { WritePrivilegeType::INSERT }, &prng, table.get(), session.get()));
    ASSERT_OK(txn->Commit());
    Status txn_status;
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &txn_status));
    ASSERT_TRUE(is_complete);
    ASSERT_OK(txn_status);

    int64_t row_count = 0;
    ASSERT_OK(CountRows(table.get(), &row_count));
    ASSERT_EQ(1, row_count);
  }

  // Start and then rollback a transaction with a row inserted.
  {
    static const string kTableName = "table_rollback";
    const string table_name = Substitute("$0.$1", kDb, kTableName);
    ASSERT_OK(CreateTable(table_name, user));

    // Grant 'INSERT' privilege, so it's possible to insert a few rows.
    ASSERT_OK(harness_->GrantTablePrivilege(
        kDb, kTableName, user, "INSERT", /*admin=*/false, cluster_));
    // Grant 'SELECT' to allow for counting rows in the table.
    ASSERT_OK(harness_->GrantTablePrivilege(
        kDb, kTableName, user, "SELECT", /*admin=*/false, cluster_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(user_client->OpenTable(table_name, &table));

    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(user_client->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::FlushMode::MANUAL_FLUSH));
    ASSERT_OK(PerformWrite(
        { WritePrivilegeType::INSERT }, &prng, table.get(), session.get()));
    ASSERT_OK(txn->Rollback());
    ASSERT_EVENTUALLY([&] {
      Status txn_status;
      bool is_complete = false;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &txn_status));
      ASSERT_TRUE(is_complete);
      ASSERT_TRUE(txn_status.IsAborted()) << txn_status.ToString();
    });

    int64_t row_count = 0;
    ASSERT_OK(CountRows(table.get(), &row_count));
    ASSERT_EQ(0, row_count);
  }
}

// A negative test case: make sure the user API presents proper error code and
// message upon an attempt to commit a transaction with non-authorized
// operations (for now, only INSERT) even in MANUAL_FLUSH mode. That's to make
// sure that the automatic flush of buffered operation upon call of
// KuduTransaction::Commit() is performed under the proper user credentials.
TEST_P(TSAuthzTxnOpsITest, CommitForNonauthorizedUsers) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const string user = "user0";
  ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
  ASSERT_OK(cluster_->kdc()->Kinit(user));

  shared_ptr<KuduClient> user_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &user_client));
  ThreadSafeRandom prng(SeedRandom());

  // Start and then commit a transaction with a single row inserted.
  {
    static const string kTableName = "table_commit_non_authorized";
    const string table_name = Substitute("$0.$1", kDb, kTableName);
    ASSERT_OK(CreateTable(table_name, user));

    // Grant only 'SELECT' privilege, so it's not possible to insert into
    // the test table.
    ASSERT_OK(harness_->GrantTablePrivilege(
        kDb, kTableName, user, "SELECT", /*admin=*/false, cluster_));
    shared_ptr<KuduTable> table;
    ASSERT_OK(user_client->OpenTable(table_name, &table));

    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(user_client->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(KuduSession::FlushMode::MANUAL_FLUSH));
    ASSERT_OK(PerformWrite(
        { WritePrivilegeType::INSERT }, &prng, table.get(), session.get()));
    const auto s = txn->Commit();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    vector<KuduError*> errors;
    session->GetPendingErrors(&errors, nullptr);
    ElementDeleter deleter(&errors);
    ASSERT_EQ(1, errors.size());
    const auto& err_status = errors[0]->status();
    ASSERT_TRUE(err_status.IsRemoteError()) << err_status.ToString();
    ASSERT_STR_CONTAINS(err_status.ToString(),
                        "Not authorized: not authorized to write");

    // However, it should be possible to rollback the transaction: that's
    // a no-op in this case.
    ASSERT_OK(txn->Rollback());

    ASSERT_EVENTUALLY([&] {
      Status txn_status;
      bool is_complete = false;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &txn_status));
      ASSERT_TRUE(is_complete);
      ASSERT_TRUE(txn_status.IsAborted()) << txn_status.ToString();
    });

    int64_t row_count = 0;
    ASSERT_OK(CountRows(table.get(), &row_count));
    ASSERT_EQ(0, row_count);
  }
}

INSTANTIATE_TEST_SUITE_P(AuthzProviders, TSAuthzTxnOpsITest,
    ::testing::Values(kRanger),
    [] (const testing::TestParamInfo<TSAuthzITest::ParamType>& info) {
      return HarnessEnumToString(info.param);
    });

} // namespace kudu
