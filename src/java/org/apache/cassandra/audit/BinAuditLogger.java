/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.audit;

import java.nio.file.Paths;

import com.google.common.primitives.Ints;

import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.fullquerylog.FullQueryLogger;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

public class BinAuditLogger extends FullQueryLogger implements IAuditLogger
{
    public BinAuditLogger()
    {
        this.configure(Paths.get(DatabaseDescriptor.getAuditLoggingOptions().audit_logs_dir),
                                            DatabaseDescriptor.getAuditLoggingOptions().roll_cycle,
                                            DatabaseDescriptor.getAuditLoggingOptions().block,
                                            DatabaseDescriptor.getAuditLoggingOptions().max_queue_weight,
                                            DatabaseDescriptor.getAuditLoggingOptions().max_log_size);
    }

    @Override
    public void log(AuditLogEntry logMessage)
    {
        if(logMessage != null)
        {
            this.log(logMessage.toString());
        }
    }

    @Override
    public void error(AuditLogEntry logMessage)
    {

        if(logMessage != null)
        {
            this.log(logMessage.toString());
        }
    }


    /**
     * Log AuditLog message
     * @param message Audit Log Message
     */
    public void log(String message)
    {
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }
        super.logRecord(new WeighableMarshallableMessage(message), binLog);
    }




    static class WeighableMarshallableMessage extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable
    {
        private final String message;

        WeighableMarshallableMessage(String message)
        {
            this.message = message;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("type").text("AuditLog");
            wire.write("message").text(message);
        }

        @Override
        public void release()
        {

        }
        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(message));
        }
    }

}
