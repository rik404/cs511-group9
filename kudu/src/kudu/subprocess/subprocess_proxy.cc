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

#include "kudu/subprocess/subprocess_proxy.h"

#include <string>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

namespace {
// Helpers to generate log4j2 properties suitable for a Java-based subprocess.
// TODO(awong): if our logging requirements get more complex, we should
// consider a builder for these.

// $0: appender instance
// $1: appender name
// $2: log directory
// $3: log filename base - new logs will be called "{log directory}/{log
//     filename base}.log" and will roll to include the date in the name
// $4: rollover policy size in MB
// $5: number of files max
const char* kLog4j2RollingPropertiesTemplate = R"(
appender.$0.type = RollingFile
appender.$0.name = $1
appender.$0.layout.type = PatternLayout
appender.$0.layout.pattern = %d{yyyy-MM-dd HH:mm:ss.SSS} [%p - %t] (%F:%L) %m%n
appender.$0.filename = $2/$3.log
appender.$0.filePattern = $2/$3.%d{yyyyMMdd-HHmmss}.log.gz
appender.$0.policies.type = Policies
appender.$0.policies.size.type = SizeBasedTriggeringPolicy
appender.$0.policies.size.size = $4 MB
appender.$0.strategy.type = DefaultRolloverStrategy
appender.$0.strategy.max = $5
)";

// $0: appender instance
// $1: appender name
const char* kLog4j2ConsoleProperties = R"(
appender.$0.type = Console
appender.$0.name = $1
appender.$0.layout.type = PatternLayout
appender.$0.layout.pattern = %d{yyyy-MM-dd HH:mm:ss.SSS} [%p - %t] (%F:%L) %m%n
)";

// $0: name of the creator of these properties (e.g. the program name)
// $1: comma-separated list of appender instances
// $2: newline-separated list of appender configs
// $3: log level (supports "all", "debug", "info", "warn", "error", "fatal")
// $4: comma-separated list of appender refs
// $5: newline-separated list of appender ref name specifications
const char* kLog4j2PropertiesTemplate = R"(
# THIS FILE WAS GENERATED BY $0.

status = error
name = PropertiesConfig
appenders = $1
$2
rootLogger.level = $3
rootLogger.appenderRefs = $4
$5
)";
} // anonymous namespace

string Log4j2Properties(const string& creator, const string& log_dir,
                        const string& log_filename, int rollover_size_mb,
                        int max_files, const string& log_level,
                        bool log_to_stdout) {
  static const char* kRollingInstance = "rollingFile";
  static const char* kRollingName = "RollingFileAppender";
  static const char* kRollingRef = "rolling";
  vector<string> appender_instances = { kRollingInstance };
  vector<string> appender_names = { kRollingName };
  vector<string> appender_configs = {
    Substitute(kLog4j2RollingPropertiesTemplate, kRollingInstance, kRollingName,
               log_dir, log_filename, rollover_size_mb, max_files)
  };
  vector<string> appender_refs = { kRollingRef };
  if (log_to_stdout) {
    static const char* kConsoleInstance = "console";
    static const char* kConsoleName = "SystemOutAppender";
    static const char* kConsoleRef = "stdout";
    appender_instances.emplace_back(kConsoleInstance);
    appender_names.emplace_back(kConsoleName);
    appender_refs.emplace_back(kConsoleRef);
    appender_configs.emplace_back(
        strings::Substitute(kLog4j2ConsoleProperties, kConsoleInstance, kConsoleName));
  }

  DCHECK_EQ(appender_refs.size(), appender_names.size());
  vector<string> appender_ref_name_specs(appender_refs.size());
  for (int i = 0; i < appender_refs.size(); i++) {
    appender_ref_name_specs.emplace_back(
        strings::Substitute("rootLogger.appenderRef.$0.ref = $1",
                            appender_refs[i], appender_names[i]));
  }
  return Substitute(kLog4j2PropertiesTemplate, creator,
                    JoinStrings(appender_instances, ", "),
                    JoinStrings(appender_configs, "\n"),
                    log_level,
                    JoinStrings(appender_refs, ", "),
                    JoinStrings(appender_ref_name_specs, "\n"));
}

}  // namespace subprocess
}  // namespace kudu
