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

#include "kudu/rpc/proxy.h"

#include <functional>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h" // IWYU pragma: keep
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/notification.h"
#include "kudu/util/status.h"
#include "kudu/util/user.h"

using google::protobuf::Message;
using std::make_shared;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {

Proxy::Proxy(shared_ptr<Messenger> messenger,
             const Sockaddr& remote,
             string hostname,
             string service_name)
    : service_name_(std::move(service_name)),
      dns_resolver_(nullptr),
      messenger_(std::move(messenger)),
      is_started_(false) {
  DCHECK(messenger_);
  DCHECK(!service_name_.empty()) << "Proxy service name must not be blank";
  DCHECK(remote.is_initialized());
  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string real_user;
  if (auto s = GetLoggedInUser(&real_user); !s.ok()) {
    LOG(WARNING) << Substitute("$0: unable to get logged-in username "
                               "before connecting to remote $1 via $2 proxy",
                               s.ToString(), remote.ToString(), service_name_);
  }

  UserCredentials creds;
  creds.set_real_user(std::move(real_user));
  conn_id_ = ConnectionId(remote, std::move(hostname), std::move(creds));
}

Proxy::Proxy(shared_ptr<Messenger> messenger,
             HostPort hp,
             DnsResolver* dns_resolver,
             string service_name)
    : service_name_(std::move(service_name)),
      hp_(std::move(hp)),
      dns_resolver_(dns_resolver),
      messenger_(std::move(messenger)),
      is_started_(false) {
  DCHECK(messenger_);
  DCHECK(!service_name_.empty()) << "Proxy service name must not be blank";
  DCHECK(hp_.Initialized());
}

Sockaddr* Proxy::GetSingleSockaddr(vector<Sockaddr>* addrs) const {
  DCHECK(!addrs->empty());
  if (PREDICT_FALSE(addrs->size() > 1)) {
    LOG(WARNING) << Substitute(
        "$0 proxy host/port $1 resolves to $2 different addresses. Using $3",
        service_name_, hp_.ToString(), addrs->size(), (*addrs)[0].ToString());
  }
  return &(*addrs)[0];
}

void Proxy::Init(Sockaddr addr) {
  if (!dns_resolver_) {
    return;
  }
  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string real_user;
  Status s = GetLoggedInUser(&real_user);
  if (!s.ok()) {
    LOG(WARNING) << Substitute(
      "$0: unable to get logged-in username before connecting to $1 via $2 proxy",
      s.ToString(), hp_.ToString(), service_name_);
  }
  vector<Sockaddr> addrs;
  if (!addr.is_initialized()) {
    s = dns_resolver_->ResolveAddresses(hp_, &addrs);
    if (PREDICT_TRUE(s.ok() && !addrs.empty())) {
      addr = *GetSingleSockaddr(&addrs);
      DCHECK(addr.is_initialized());
      addr.set_port(hp_.port());
      // NOTE: it's ok to proceed on failure -- the address will remain
      // uninitialized and be re-resolved when sending the next request.
    }
  }

  UserCredentials creds;
  creds.set_real_user(std::move(real_user));
  conn_id_ = ConnectionId(addr, hp_.host(), std::move(creds));
}

Proxy::~Proxy() {
}

void Proxy::EnqueueRequest(const string& method,
                           unique_ptr<RequestPayload> req_payload,
                           google::protobuf::Message* response,
                           RpcController* controller,
                           const ResponseCallback& callback,
                           OutboundCall::CallbackBehavior cb_behavior) const {
  ConnectionId connection = conn_id();
  DCHECK(connection.remote().is_initialized());
  controller->call_ = make_shared<OutboundCall>(
      connection,
      RemoteMethod{service_name_, method},
      std::move(req_payload),
      cb_behavior,
      response,
      controller,
      callback);
  controller->SetMessenger(messenger_.get());

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->QueueOutboundCall(controller->call_);
}

void Proxy::RefreshDnsAndEnqueueRequest(const string& method,
                                        unique_ptr<RequestPayload> req_payload,
                                        google::protobuf::Message* response,
                                        RpcController* controller,
                                        const ResponseCallback& callback) {
  DCHECK(!controller->call_);
  vector<Sockaddr>* addrs = new vector<Sockaddr>();
  DCHECK_NOTNULL(dns_resolver_)->RefreshAddressesAsync(hp_, addrs,
      [this, req_raw = req_payload.release(),
       &method, callback, response, controller, addrs] (const Status& s) mutable {
    unique_ptr<RequestPayload> req_payload(req_raw);
    unique_ptr<vector<Sockaddr>> unique_addrs(addrs);
    // If we fail to resolve the address, treat the call as failed.
    if (!s.ok() || addrs->empty()) {
      DCHECK(!controller->call_);
      // NOTE: we need to keep a reference here because the callback may end up
      // destructing the controller and the outbound call, _while_ the callback
      // is running from within the call!
      auto shared_call = make_shared<OutboundCall>(
          conn_id(), RemoteMethod{service_name_, method}, response, controller, callback);
      controller->call_ = shared_call;
      controller->call_->SetFailed(s.CloneAndPrepend("failed to refresh physical address"));
      return;
    }
    auto* addr = GetSingleSockaddr(addrs);
    DCHECK(addr->is_initialized());
    addr->set_port(hp_.port());
    {
      std::lock_guard<simple_spinlock> l(lock_);
      conn_id_.set_remote(*addr);
    }
    // NOTE: we don't expect the user-provided callback to free sidecars, so
    // make sure the outbound call frees it for us.
    EnqueueRequest(method, std::move(req_payload), response, controller, callback,
                   OutboundCall::CallbackBehavior::kFreeSidecars);
  });
}

void Proxy::AsyncRequest(const string& method,
                         const google::protobuf::Message& req,
                         google::protobuf::Message* response,
                         RpcController* controller,
                         const ResponseCallback& callback) {
  DCHECK(!controller->call_) << "Controller should be reset";
  base::subtle::NoBarrier_Store(&is_started_, true);
  // TODO(awong): it would be great if we didn't have to heap allocate the
  // payload.
  auto req_payload = RequestPayload::CreateRequestPayload(
      RemoteMethod{service_name_, method},
      req, controller->ReleaseOutboundSidecars());
  if (!dns_resolver_) {
    // NOTE: we don't expect the user-provided callback to free sidecars, so
    // make sure the outbound call frees it for us.
    EnqueueRequest(method, std::move(req_payload), response, controller, callback,
                   OutboundCall::CallbackBehavior::kFreeSidecars);
    return;
  }

  // If we haven't successfully initialized the remote, e.g. because the DNS
  // lookup failed, refresh the DNS entry and enqueue the request.
  bool remote_initialized;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    remote_initialized = conn_id_.remote().is_initialized();
  }
  if (!remote_initialized) {
    RefreshDnsAndEnqueueRequest(method, std::move(req_payload), response, controller, callback);
    return;
  }

  // Otherwise, just enqueue the request, but retry if there's an error, since
  // it's possible the physical address of the host was changed. We only retry
  // once more before calling the callback.
  auto refresh_dns_and_cb = [this, &method, callback, response, controller] () {
    // TODO(awong): we should be more specific here -- consider having the RPC
    // layer set a flag in the controller that warrants a retry.
    if (PREDICT_FALSE(!controller->status().ok())) {
      KLOG_EVERY_N_SECS(WARNING, 5)
          << Substitute("Call had error, refreshing address and retrying: $0",
                        controller->status().ToString()) << THROTTLE_MSG;
      auto req_payload = controller->ReleaseRequestPayload();
      controller->Reset();
      RefreshDnsAndEnqueueRequest(method, std::move(req_payload), response, controller, callback);
      return;
    }
    // For any other status, OK or otherwise, just run the callback.
    controller->FreeOutboundSidecars();
    SCOPED_WATCH_STACK(100);
    callback();
  };
  // Since we may end up using the request payload in the event of a retry,
  // ensure the outbound call doesn't free the sidecars, and instead free
  // manually from within our callback.
  EnqueueRequest(method, std::move(req_payload), response, controller, refresh_dns_and_cb,
                 OutboundCall::CallbackBehavior::kDontFreeSidecars);
}


Status Proxy::SyncRequest(const string& method,
                          const google::protobuf::Message& req,
                          google::protobuf::Message* resp,
                          RpcController* controller) {
  Notification note;
  AsyncRequest(method, req, DCHECK_NOTNULL(resp), controller,
               [&note]() { note.Notify(); });
  note.WaitForNotification();
  return controller->status();
}

void Proxy::set_user_credentials(UserCredentials user_credentials) {
  DCHECK(base::subtle::NoBarrier_Load(&is_started_) == false)
      << "illegal to call set_user_credentials() after request processing has started";
  conn_id_.set_user_credentials(std::move(user_credentials));
}

void Proxy::set_network_plane(string network_plane) {
  DCHECK(base::subtle::NoBarrier_Load(&is_started_) == false)
      << "illegal to call set_network_plane() after request processing has started";
  conn_id_.set_network_plane(std::move(network_plane));
}

string Proxy::ToString() const {
  return Substitute("$0@$1", service_name_, conn_id_.ToString());
}

} // namespace rpc
} // namespace kudu
