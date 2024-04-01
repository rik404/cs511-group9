#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import division

import json
import os
import subprocess

import kudu
from kudu.client import Partitioning

# There's no built-in timeout error in Python 2.
# See https://stackoverflow.com/questions/2281850.
class TimeoutError(Exception):
    pass

class KuduTestBase(object):

    """
    Base test class that will start a configurable number of master and
    tablet servers.
    """

    NUM_MASTER_SERVERS = 3
    NUM_TABLET_SERVERS = 3

    valid_account_id = "valid_account_id"
    invalid_account_id = "invalid_account_id"

    @classmethod
    def send_and_receive(cls, proc, request):
        binary_req = (json.dumps(request) + "\n").encode("utf-8")
        proc.stdin.write(binary_req)
        proc.stdin.flush()
        binary_resp = proc.stdout.readline()
        response = json.loads(binary_resp[:-1].decode("utf-8"))
        if "error" in response:
            raise Exception("Error in response: {0}".format(response["error"]))
        return response

    @classmethod
    def start_cluster(cls):
        kudu_build = os.getenv("KUDU_BUILD")
        if not kudu_build:
            kudu_build = os.path.join(os.getenv("KUDU_HOME"), "build", "latest")
        bin_path = "{0}/bin".format(kudu_build)

        master_hosts = []
        master_ports = []
        master_http_hostports = []

        # Start the mini-cluster control process.
        args = ["{0}/kudu".format(bin_path), "test", "mini_cluster"]
        p = subprocess.Popen(args, shell=False,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        # Create and start a cluster.
        #
        # Only make one replica so that our tests don't need to worry about
        # setting consistency modes.
        #
        # By default, components of the external mini-cluster harness are run
        # with shortest keys to save CPU resources and speed up tests. In
        # contemporary or security-hardened OS distros that requires customizing
        # OpenSSL's security level at the client side, lowering it down to 0
        # (otherwise, the client side rejects certificates signed by
        # not-strong-enough keys). Since customization of the security level
        # for the kudu-client library via gflags is not trivial, let's override
        # the length of the RSA keys used for CA and server certificates,
        # making them acceptable even at OpenSSL's security level 2.
        cls.send_and_receive(
            p, { "create_cluster" :
                 { "numMasters" : cls.NUM_MASTER_SERVERS,
                   "numTservers" : cls.NUM_TABLET_SERVERS,
                   "extraMasterFlags" : [
                       "--default_num_replicas=1",
                       "--ipki_ca_key_size=2048",
                       "--ipki_server_key_size=2048",
                       # TODO: once setting flags per unittest is implemented,
                       # remove this line here and add it to the test:
                       # 'test_soft_delete_and_recall_table_after_reserve_time'
                       "--check_expired_table_interval_seconds=2" ],
                   "extraTserverFlags" : [ "--ipki_server_key_size=2048" ],
                   "mini_oidc_options" :
                   { "expiration_time" : "300000",
                     "jwks_options" :
                     [{ "account_id" : cls.valid_account_id,
                       "is_valid_key" : "true" },
                       { "account_id" : cls.invalid_account_id,
                       "is_valid_key" : "false" },
                       ]}}})
        cls.send_and_receive(p, { "start_cluster" : {}})

        # Get information about the cluster's masters.
        masters = cls.send_and_receive(p, { "get_masters" : {}})
        for m in masters["getMasters"]["masters"]:
            master_hosts.append(m["boundRpcAddress"]["host"])
            master_ports.append(m["boundRpcAddress"]["port"])
            master_http_hostports.append(
                '{0}:{1}'.format(m["boundHttpAddress"]["host"],
                                 m["boundHttpAddress"]["port"]))

        return p, master_hosts, master_ports, master_http_hostports

    @classmethod
    def stop_cluster(cls):
        cls.cluster_proc.stdin.close()
        ret = cls.cluster_proc.wait()
        if ret != 0:
            raise Exception("Minicluster process exited with code {0}".format(ret))

    @classmethod
    def setUpClass(cls):
        cls.cluster_proc, cls.master_hosts, cls.master_ports, \
                cls.master_http_hostports = cls.start_cluster()

        cls.client = kudu.connect(cls.master_hosts, cls.master_ports)

        cls.schema = cls.example_schema()
        cls.partitioning = cls.example_partitioning()

        cls.ex_table = 'example-table'
        if cls.client.table_exists(cls.ex_table):
            cls.client.delete_table(cls.ex_table)
        cls.client.create_table(cls.ex_table, cls.schema, cls.partitioning)

    @classmethod
    def tearDownClass(cls):
        cls.stop_cluster()

    @classmethod
    def example_schema(cls):
        builder = kudu.schema_builder()
        builder.add_column('key', kudu.int32, nullable=False, comment='key_comment')
        builder.add_column('int_val', kudu.int32)
        builder.add_column('string_val', kudu.string, default=None)
        builder.add_column('unixtime_micros_val', kudu.unixtime_micros)
        builder.set_primary_keys(['key'])

        return builder.build()

    @classmethod
    def example_partitioning(cls):
        return Partitioning().set_range_partition_columns(['key'])

    @classmethod
    def get_jwt(cls, valid=True):
        account_id = cls.valid_account_id
        is_valid_key = valid
        if not valid:
            account_id = cls.invalid_account_id

        resp = cls.send_and_receive(
            cls.cluster_proc, { "create_jwt" : {
                "account_id" : account_id,
                "subject" : "test",
                "is_valid_key" : is_valid_key}})
        return resp['createJwt']['jwt']

    @classmethod
    def doVerifyMetrics(cls, session,
        successful_inserts,
        insert_ignore_errors,
        successful_upserts,
        upsert_ignore_errors,
        successful_updates,
        update_ignore_errors,
        successful_deletes,
        delete_ignore_errors,):
        metrics = session.get_write_op_metrics()
        assert successful_inserts == metrics["successful_inserts"]
        assert insert_ignore_errors == metrics["insert_ignore_errors"]
        assert successful_upserts == metrics["successful_upserts"]
        assert upsert_ignore_errors == metrics["upsert_ignore_errors"]
        assert successful_updates == metrics["successful_updates"]
        assert update_ignore_errors == metrics["update_ignore_errors"]
        assert successful_deletes == metrics["successful_deletes"]
        assert delete_ignore_errors == metrics["delete_ignore_errors"]
