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

#include <openssl/ssl.h>

#include <cstdint>
#include <string>

#include "kudu/gutil/port.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/openssl_util.h" // IWYU pragma: keep
#include "kudu/util/status.h"

struct iovec;

typedef struct ssl_st SSL;

namespace kudu {

class TransportDetailsPB;

namespace security {

class TlsSocket : public Socket {
 public:

  ~TlsSocket() override;

  Status Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) override WARN_UNUSED_RESULT;

  Status Writev(const struct ::iovec *iov,
                int iov_len,
                int64_t *nwritten) override WARN_UNUSED_RESULT;

  Status Recv(uint8_t *buf, int32_t amt, int32_t *nread) override WARN_UNUSED_RESULT;

  Status Close() override WARN_UNUSED_RESULT;

  Status GetTransportDetails(TransportDetailsPB* pb) const override;

  // Get the name of the negotiated TLS protocol for the connection.
  std::string GetProtocolName() const;

  // Get the description of the negotiated TLS cipher suite for the connection.
  std::string GetCipherDescription() const;

 private:

  friend class TlsHandshake;

  TlsSocket(int fd, c_unique_ptr<SSL> ssl);

  // Owned SSL handle.
  c_unique_ptr<SSL> ssl_;

  bool use_cork_;

  // Socket-local buffer used by Writev().
  faststring buf_;
};

} // namespace security
} // namespace kudu
