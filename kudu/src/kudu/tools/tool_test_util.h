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
//
// Utility functions for generating data for use by tools and tests.

#pragma once

#include <map>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {
namespace tools {

enum class Kudu1097 {
  Disable,
  Enable,
};

// Get full path to the top-level 'kudu' tool binary.
std::string GetKuduToolAbsolutePath();

// Runs the 'kudu' tool binary with the given arguments.
//
// If 'out' or 'err' is set, the tool's stdout or stderr output will be
// written to each respectively.
// Optionally allows a passed map of environment variables to be set
// on the kudu tool via 'env_vars'.
Status RunKuduTool(const std::vector<std::string>& args,
                   std::string* out = nullptr,
                   std::string* err = nullptr,
                   const std::string& in = "",
                   std::map<std::string, std::string> env_vars = {});

// Runs the 'kudu' tool binary with the given argument string, returning an
// error prepended with stdout and stderr if the run was unsuccessful.
Status RunActionPrependStdoutStderr(const std::string& arg_str);

} // namespace tools
} // namespace kudu
