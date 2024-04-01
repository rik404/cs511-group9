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

#include <string>
#include <utility>

#include "kudu/fs/key_provider.h"
#include "kudu/ranger-kms/ranger_kms_client.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {
class RangerKMSKeyProvider : public KeyProvider {
 public:
  ~RangerKMSKeyProvider() override {}

  RangerKMSKeyProvider(const std::string& kms_url, std::string cluster_key_name)
      : client_(kms_url, std::move(cluster_key_name)) {}

  // Decrypts the encryption key.
  Status DecryptEncryptionKey(const std::string& encryption_key,
                              const std::string& iv,
                              const std::string& key_version,
                              std::string* decrypted_key) override;

  // Generates an encryption key (the generated key is encrypted).
  Status GenerateEncryptionKey(std::string* encryption_key,
                               std::string* iv,
                               std::string* key_version) override;

  // Generates an encryption key with the tenant_id.
  Status GenerateTenantKey(const std::string& tenant_id,
                           std::string* encryption_key,
                           std::string* iv,
                           std::string* key_version) override;
 private:
  RangerKMSClient client_;
};
} // namespace security
} // namespace kudu
