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
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {
class MonoDelta;
}  // namespace kudu

namespace kudu {
namespace cloud {

enum class CloudType {
  AWS,
  AZURE,
  GCE,
  OPENSTACK,
};

const char* TypeToString(CloudType type);

// A class to collect metadata of a public cloud instance. It includes a generic
// interface and common base stuff to work with HTTP-based metadata server.
// That's the ubiquitous way of accessing metadata from within a cloud instance
// such as AWS, GCE, DigitalOcean.
//
// Concrete classes implementing this interface use stable APIs to retrieve
// corresponding information (published by corresponding cloud providers).
//
// NOTE:
//   It's assumed Kudu processes can access the metadata service (i.e. query
//   particular URLs of a metadata server) without specifying any credentials.
//   In other words, the metadata server should not be firewalled or hardened
//   with access controls to allow this code to work.
class InstanceMetadata {
 public:
  InstanceMetadata();
  virtual ~InstanceMetadata() = default;

  // Initialize the object, collecting information about a cloud instance.
  // It's a synchronous call and it can take some time to complete.
  // If the basic information has been retrieved successfully, returns
  // Status::OK(), otherwise returns non-OK status to reflect the error
  // encountered.
  virtual Status Init() WARN_UNUSED_RESULT;

  // Get the type of the cloud instance.
  virtual CloudType type() const = 0;

  // Get the internal NTP server accessible from within the instance.
  // On success, returns Status::OK() and populates the output parameter
  // 'server' with IP address or FQDN of the NTP server available from within
  // the instance. Returns
  //   * Status::NotSupported() if the cloud platform doesn't provide internal
  //                            NTP service for its instances
  //   * Status::IllegalState() if the metadata object requires initialization,
  //                            but it hasn't been initialized yet
  virtual Status GetNtpServer(std::string* server) const WARN_UNUSED_RESULT = 0;

 protected:
  // Fetch data from specified URL and output into the 'out' parameter. This
  // method is targeted for fetching information from the instance's metadata
  // server. The 'out' output parameter can be null.
  static Status Fetch(const std::string& url,
                      MonoDelta timeout,
                      const std::vector<std::string>& headers,
                      std::string* out = nullptr);

  // The timeout used for HTTP requests sent to the metadata server. The base
  // implementation assumes the metadata server is robust enough to respond
  // in a fraction of a second. If not, override this method accordingly.
  virtual MonoDelta request_timeout() const;

  // Return HTTP header fields to supply with requests to the metadata server.
  // Metadata servers might have specific requirements on expected headers.
  virtual const std::vector<std::string>& request_headers() const = 0;

  // Return metadata server's URL used to retrieve instance identifier.
  // It's assumed the server replies with plain text, where the string
  // contains just the identifier.
  virtual const std::string& instance_id_url() const = 0;

 private:
  // Whether this object has been initialized.
  bool is_initialized_;
};

// More information on the metadata server for EC2 cloud instances:
//   https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ \
//     ec2-instance-metadata.html
class AwsInstanceMetadata : public InstanceMetadata {
 public:
  AwsInstanceMetadata() = default;
  ~AwsInstanceMetadata() = default;

  Status Init() override WARN_UNUSED_RESULT;

  CloudType type() const override { return CloudType::AWS; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

// More information on the metadata server for Azure cloud instances:
//   https://docs.microsoft.com/en-us/azure/virtual-machines/linux/ \
//     instance-metadata-service
class AzureInstanceMetadata : public InstanceMetadata {
 public:
  AzureInstanceMetadata() = default;
  ~AzureInstanceMetadata() = default;

  CloudType type() const override { return CloudType::AZURE; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

// More information on the metadata server for GCE cloud instances:
//   https://cloud.google.com/compute/docs/storing-retrieving-metadata
class GceInstanceMetadata : public InstanceMetadata {
 public:
  GceInstanceMetadata() = default;
  ~GceInstanceMetadata() = default;

  CloudType type() const override { return CloudType::GCE; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

// More information on Nova metadata server for OpenStack cloud instances is at:
//   https://docs.openstack.org/nova/latest/user/metadata.html#metadata-service
//
// TODO(aserbin): when necessary, implement extracting instance uuid out of the
//                meta_data.json content fetched from
//                FLAGS_cloud_openstack_metadata_url.
class OpenStackInstanceMetadata : public InstanceMetadata {
 public:
  OpenStackInstanceMetadata() = default;
  ~OpenStackInstanceMetadata() = default;

  CloudType type() const override { return CloudType::OPENSTACK; }
  Status GetNtpServer(std::string* server) const override WARN_UNUSED_RESULT;

 protected:
  const std::vector<std::string>& request_headers() const override;
  const std::string& instance_id_url() const override;
};

} // namespace cloud
} // namespace kudu
