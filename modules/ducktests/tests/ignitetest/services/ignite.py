# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os.path
import signal

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from ignitetest.services.utils.ignite_config import IgniteConfig
from ignitetest.services.utils.ignite_path import IgnitePath
from ignitetest.version import DEV_BRANCH


class IgniteService(Service):
    PERSISTENT_ROOT = "/mnt/ignite"
    WORK_DIR = os.path.join(PERSISTENT_ROOT, "work")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "ignite-config.xml")
    LOG4J_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "ignite-log4j.xml")
    HEAP_DUMP_FILE = os.path.join(PERSISTENT_ROOT, "ignite-heap.bin")
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console.log")

    logs = {
        "console_log": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True},

        "heap_dump": {
            "path": HEAP_DUMP_FILE,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes=3, version=DEV_BRANCH):
        """
        :param context: test context
        :param num_nodes: number of Ignite nodes.
        """
        Service.__init__(self, context, num_nodes)

        self.log_level = "DEBUG"
        self.config = IgniteConfig()
        self.path = IgnitePath()

        for node in self.nodes:
            node.version = version

    def start(self):
        Service.start(self)

        self.logger.info("Waiting for Ignite to start...")

    def start_cmd(self, node):
        jvm_opts = "-J-DIGNITE_SUCCESS_FILE=" + IgniteService.PERSISTENT_ROOT + "/success_file "
        jvm_opts += "-J-Dlog4j.configDebug=true"

        cmd = "export EXCLUDE_TEST_CLASSES=true; "
        cmd += "export IGNITE_LOG_DIR=" + IgniteService.PERSISTENT_ROOT + "; "
        cmd += "%s %s %s 1>> %s 2>> %s &" % \
              (self.path.script("ignite.sh", node),
               jvm_opts,
               IgniteService.CONFIG_FILE,
               IgniteService.STDOUT_STDERR_CAPTURE,
               IgniteService.STDOUT_STDERR_CAPTURE)
        return cmd

    def start_node(self, node, timeout_sec=180, wait_for_rebalance=False):
        node.account.mkdirs(IgniteService.PERSISTENT_ROOT)
        node.account.create_file(IgniteService.CONFIG_FILE,
                                 self.config.render(IgniteService.PERSISTENT_ROOT, IgniteService.WORK_DIR))
        node.account.create_file(IgniteService.LOG4J_CONFIG_FILE, self.config.render_log4j(IgniteService.WORK_DIR))

        cmd = self.start_cmd(node)
        self.logger.debug("Attempting to start IgniteService on %s with command: %s" % (str(node.account), cmd))

        wait_for_message = "Topology snapshot"
        if wait_for_rebalance:
            wait_for_message = "Completed (final) rebalancing \[grp=test-cache"

        with node.account.monitor_log(IgniteService.STDOUT_STDERR_CAPTURE) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until(wait_for_message, timeout_sec=timeout_sec, backoff_sec=5,
                               err_msg="Ignite server didn't finish startup in %d seconds" % timeout_sec)

        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    def stop_node(self, node, clean_shutdown=True, timeout_sec=60):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=False)

        try:
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=timeout_sec,
                       err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            self.thread_dump(node)
            raise

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % IgniteService.PERSISTENT_ROOT, allow_fail=False)

    def thread_dump(self, node):
        for pid in self.pids(node):
            try:
                node.account.signal(pid, signal.SIGQUIT, allow_fail=True)
            except:
                self.logger.warn("Could not dump threads on node")

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "jcmd | grep -e %s | awk '{print $1}'" % self.java_class_name()
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def java_class_name(self):
        return "org.apache.ignite.startup.cmdline.CommandLineStartup"

    def set_version(self, version):
        for node in self.nodes:
            node.version = version

    def alive(self, node):
        return len(self.pids(node)) > 0

