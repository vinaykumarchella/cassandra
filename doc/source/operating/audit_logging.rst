.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none

Audit Logging
------------------

Audit logging in Cassandra logs every incoming CQL command request, Authentication (successful as well as unsuccessful login)
to C* node. Currently, there are two implementations provided, the custom logger can be implemented and injected with the
class name as a parameter in cassandra.yaml.

- ``FileAuditLogger`` Logs events to  ``audit/audit.log`` file using slf4j logger.
- ``BinAuditLogger`` An efficient way to log events to ``${cassandra.logdir.audit}``
   or ``${cassandra.logdir}/audit/`` directory in binary format.


What does it capture
^^^^^^^^^^^^^^^^^

Audit logging captures following events

- Successful as well as unsuccessful login attempts.

- All database commands executed via Native protocol (CQL) attempted or successfully executed.

What does it log
^^^^^^^^^^^^^^^^^
It logs following attributes
 - ``user``: User name(if available)
 - ``host``: Host IP, where the command is being executed
 - ``source ip address``: Source IP address from where the request initiated
 - ``source port``: Source port number from where the request initiated
 - ``timestamp``: unix time stamp
 - ``type``: Type of the request (SELECT, INSERT, etc.,)
 - ``category`` - Category of the request (DDL, DML, etc.,)
 - ``keyspace`` - Keyspace(If applicable) on which request is targeted to be executed
 - ``scope`` - Table/Aggregate name/ function name/ trigger name etc., as applicable
 - ``operation`` - CQL command being executed

How to configure
^^^^^^^^^^^^^^^^^
enabled: This option enables/ disables audit log
``logger``: Class name of the logger/ custom logger.
``included_keyspaces``: Comma separated list of keyspaces to be included in audit log, default - includes all keyspaces
``excluded_keyspaces``: Comma separated list of keyspaces to be excluded from audit log, default - excludes no keyspace
``included_categories``: Comma separated list of Audit Log Categories to be included in audit log, default - includes all categories
``excluded_categories``: Comma separated list of Audit Log Categories to be excluded from audit log, default - excludes no category
``included_users``: Comma separated list of users to be included in audit log, default - includes all users
``excluded_users``: Comma separated list of users to be excluded from audit log, default - excludes no user
  List of available categories are found in org.apache.cassandra.audit.AuditLogEntryType.java file

Sample output
^^^^^^^^^^^^^^^^^
::

    LogMessage: user:anonymous|host:localhost/X.X.X.X|source:/X.X.X.X|port:60878|timestamp:1521158923615|type:USE_KS|category:DDL|ks:dev1|operation:USE "dev1"

