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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.StorageService;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;


public class AuditLoggerTest extends CQLTester
{

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setAuditLogEnabled(true);
        AuditLogOptions options = new AuditLogOptions();
        options.enabled = true;
        options.logger = "InMemoryAuditLogger";
        DatabaseDescriptor.setAuditLoggingOptions(options);
        requireNetwork();

    }

    @Before
    public void beforeTestMethod() throws Throwable
    {
        AuditLogOptions options = new AuditLogOptions();
        options.enabled = true;
        options.logger = "InMemoryAuditLogger";
        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.reloadAuditLogFilters();

    }

    @AfterClass
    public static void tearDown()
    {
    }


    @Test
    public void testAuditLogFilters() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        AuditLogOptions options = new AuditLogOptions();
        options.enabled = true;
        options.logger = "InMemoryAuditLogger";
        options.excluded_keyspaces = KEYSPACE;
        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.reloadAuditLogFilters();

        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        ResultSet rs  = executeAndAssertNoAuditLog(cql, 1);
        assertEquals(1, rs.all().size());

        options = new AuditLogOptions();
        options.enabled = true;
        options.logger = "InMemoryAuditLogger";
        options.included_keyspaces = KEYSPACE;
        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.reloadAuditLogFilters();

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs  = executeAndAssertWithPrepare(cql, AuditLogEntryType.SELECT, 1);
        assertEquals(1, rs.all().size());

        options = new AuditLogOptions();
        options.enabled = true;
        options.logger = "InMemoryAuditLogger";
        options.included_keyspaces = KEYSPACE;
        options.excluded_keyspaces = KEYSPACE;
        DatabaseDescriptor.setAuditLoggingOptions(options);
        StorageService.instance.reloadAuditLogFilters();

        cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        rs  = executeAndAssertNoAuditLog(cql, 1);
        assertEquals(1, rs.all().size());
    }


    @Test
    public void testCqlSELECTAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        ResultSet rs  = executeAndAssertWithPrepare(cql, AuditLogEntryType.SELECT, 1);

        assertEquals(1, rs.all().size());
    }

    @Test
    public void testCqlINSERTAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "  (id, v1, v2) VALUES (?, ?, ?)";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.UPDATE, 1, "insert_audit", "test");

    }

    @Test
    public void testCqlUPDATEAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "UPDATE " + KEYSPACE + '.' + currentTable() + "  SET v1 = 'ApacheCassandra' WHERE id = 1";
        executeAndAssert(cql, AuditLogEntryType.UPDATE);

        cql = "UPDATE " + KEYSPACE + '.' + currentTable() + "  SET v1 = ? WHERE id = ?";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.UPDATE, "AuditingTest", 2);

    }

    @Test
    public void testCqlDELETEAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "DELETE FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
        executeAndAssertWithPrepare(cql, AuditLogEntryType.DELETE, 1);

    }


    @Test
    public void testCqlTRUNCATEAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "TRUNCATE TABLE  " + KEYSPACE + '.' + currentTable();
        executeAndAssertWithPrepare(cql, AuditLogEntryType.TRUNCATE);

    }

    @Test
    public void testCqlBATCHAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        Session session = sessionNet();

        BatchStatement batchStatement = new BatchStatement();

        String cqlInsert = "INSERT INTO "+KEYSPACE+"."+currentTable()+" (id, v1, v2) VALUES (?, ?, ?)";
        PreparedStatement prep = session.prepare(cqlInsert);
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1,"Apapche","Cassandra"));
        batchStatement.add(prep.bind(2,"Apapche1","Cassandra1"));

        String cqlUpdate = "UPDATE "+KEYSPACE+"."+currentTable()+" SET v1 = ? WHERE id = ?";
        prep = session.prepare(cqlUpdate);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlUpdate, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind("Apache Cassandra",1));

        String cqlDelete = "DELETE FROM "+KEYSPACE+"."+currentTable()+" WHERE id = ?";
        prep = session.prepare(cqlDelete);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlDelete, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1));


        ResultSet rs = session.execute(batchStatement);

        assertEquals(5,((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert, AuditLogEntryType.UPDATE, logEntry, false);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert, AuditLogEntryType.UPDATE, logEntry, false);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlUpdate, AuditLogEntryType.UPDATE, logEntry, false);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlDelete, AuditLogEntryType.DELETE, logEntry, false);

        int size = rs.all().size();

        assertEquals(0,size);

    }

    @Test
    public void testCqlBATCH_Multiple_Tables_Auditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String table1 = currentTable();

        Session session = sessionNet();

        BatchStatement batchStatement = new BatchStatement();

        String cqlInsert1 = "INSERT INTO "+KEYSPACE+"."+table1+" (id, v1, v2) VALUES (?, ?, ?)";
        PreparedStatement prep = session.prepare(cqlInsert1);
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert1, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1,"Apapche","Cassandra"));

        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String table2 = currentTable();

        String cqlInsert2 = "INSERT INTO "+KEYSPACE+"."+table2+" (id, v1, v2) VALUES (?, ?, ?)";
        prep = session.prepare(cqlInsert2);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert2, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

        batchStatement.add(prep.bind(1,"Apapche","Cassandra"));


        createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String ks2 = currentKeyspace();

        createTable(ks2,"CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String table3 = currentTable();

        String cqlInsert3 = "INSERT INTO "+ks2+"."+table3+" (id, v1, v2) VALUES (?, ?, ?)";
        prep = session.prepare(cqlInsert3);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert3, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false, ks2);

        batchStatement.add(prep.bind(1,"Apapche","Cassandra"));


        ResultSet rs = session.execute(batchStatement);

        assertEquals(4,((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert1, table1, AuditLogEntryType.UPDATE, logEntry, false, KEYSPACE);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert2, table2, AuditLogEntryType.UPDATE, logEntry, false, KEYSPACE);

        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cqlInsert3, table3, AuditLogEntryType.UPDATE, logEntry, false, ks2);

        int size = rs.all().size();

        assertEquals(0,size);

    }



    @Test
    public void testCqlKSAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String cql = "CREATE KEYSPACE "+createKeyspaceName()+" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}  ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_KS, true, currentKeyspace());

        cql = "CREATE KEYSPACE IF NOT EXISTS "+createKeyspaceName()+" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}  ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_KS, true, currentKeyspace());


        cql = "ALTER KEYSPACE "+currentKeyspace()+" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}  ";
        executeAndAssert(cql, AuditLogEntryType.ALTER_KS, true, currentKeyspace());

        cql = "DROP KEYSPACE "+currentKeyspace();
        executeAndAssert(cql, AuditLogEntryType.DROP_KS, true, currentKeyspace());
    }

    @Test
    public void testCqlTableAuditing() throws Throwable
    {
        String cql = "CREATE TABLE "+KEYSPACE+"."+createTableName()+" (id int primary key, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TABLE);

        cql = "CREATE TABLE IF NOT EXISTS "+KEYSPACE+"."+createTableName()+" (id int primary key, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TABLE);

        cql = "ALTER TABLE "+KEYSPACE+"."+currentTable()+" ADD v3 text";
        executeAndAssert(cql, AuditLogEntryType.ALTER_TABLE);

        cql = "DROP TABLE "+KEYSPACE+"."+currentTable();
        executeAndAssert(cql, AuditLogEntryType.DROP_TABLE);
    }


    @Test
    public void testCqlMVAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String tblName = currentTable();
        String cql = "CREATE MATERIALIZED VIEW "+KEYSPACE+"."+createTableName()+" AS SELECT id,v1 FROM "+KEYSPACE+"."+tblName+" WHERE id IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY ( id, v1 ) ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_VIEW);

        cql = "CREATE MATERIALIZED VIEW IF NOT EXISTS "+KEYSPACE+"."+currentTable()+" AS SELECT id,v1 FROM "+KEYSPACE+"."+tblName+" WHERE id IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY ( id, v1 ) ";
        executeAndAssert(cql, AuditLogEntryType.CREATE_VIEW);

        cql = "ALTER MATERIALIZED VIEW "+KEYSPACE+"."+currentTable()+" WITH caching = {  'keys' : 'NONE' };";
        executeAndAssert(cql, AuditLogEntryType.ALTER_VIEW);

        cql = "DROP MATERIALIZED VIEW "+KEYSPACE+"."+currentTable();
        executeAndAssert(cql, AuditLogEntryType.DROP_VIEW);
    }



    @Test
    public void testCqlTYPEAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = createTableName();

        String cql = "CREATE TYPE "+KEYSPACE+"."+tblName+" (id int, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TYPE);

        cql = "CREATE TYPE IF NOT EXISTS "+KEYSPACE+"."+tblName+" (id int, v1 text, v2 text)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_TYPE);

        cql = "ALTER TYPE "+KEYSPACE+"."+tblName+" ADD v3 int";
        executeAndAssert(cql, AuditLogEntryType.ALTER_TYPE);

        cql = "ALTER TYPE "+KEYSPACE+"."+tblName+" RENAME v3 TO v4";
        executeAndAssert(cql, AuditLogEntryType.ALTER_TYPE);

        cql = "DROP TYPE "+KEYSPACE+"."+tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_TYPE);

        cql = "DROP TYPE IF EXISTS "+KEYSPACE+"."+tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_TYPE);
    }


    @Test
    public void testCqlINDEXAuditing() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = currentTable();

        String indexName = createTableName();

        String cql = "CREATE INDEX "+indexName+" ON "+KEYSPACE+"."+tblName+" (v1)";
        executeAndAssert(cql, AuditLogEntryType.CREATE_INDEX);

        cql = "DROP INDEX "+KEYSPACE+"."+indexName;
        executeAndAssert(cql, AuditLogEntryType.DROP_INDEX);

    }

    @Test
    public void testCqlFUNCTIONAuditing() throws Throwable
    {

        String tblName = createTableName();

        String cql = "CREATE FUNCTION IF NOT EXISTS  "+KEYSPACE+"."+tblName+" (column TEXT,num int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$ column.substring(0,num) $$";
        executeAndAssert(cql, AuditLogEntryType.CREATE_FUNC);

        cql = "DROP FUNCTION "+KEYSPACE+"."+tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_FUNC);

    }

    @Test
    public void testCqlTRIGGERAuditing() throws Throwable
    {

        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = currentTable();
        String triggerName = createTableName();

        String cql = "DROP TRIGGER IF EXISTS "+triggerName+ " ON "+KEYSPACE+"."+tblName;
        executeAndAssert(cql, AuditLogEntryType.DROP_TRIGGER);

    }


    @Test
    public void testCqlAGGREGATEAuditing() throws Throwable
    {
        String aggName = createTableName();

        String cql = "DROP AGGREGATE IF EXISTS "+KEYSPACE+"."+aggName;
        executeAndAssert(cql, AuditLogEntryType.DROP_AGG);

    }


    @Test
    public void testCqlQuerySyntaxError()
    {
        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "1 (id, v1, v2) VALUES (1, 'insert_audit, 'test')";
        try
        {
            createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

            Session session = sessionNet();

            ResultSet rs = session.execute(cql);
        }
        catch (SyntaxError e)
        {
        }
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0,((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
    }
    @Test
    public void testCqlPrepareQueryError()
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        String cql = "INSERT INTO " + KEYSPACE + '.'+currentTable()+ " (id, v1, v2) VALUES (?,?,?)";
        try
        {

            Session session = sessionNet();

            PreparedStatement pstmt = session.prepare(cql);
            AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
            assertLogEntry(cql, AuditLogEntryType.PREPARE_STATEMENT, logEntry, false);

            dropTable("DROP TABLE %s");
            ResultSet rs = session.execute(pstmt.bind(1, "insert_audit", "test"));

        }
        catch (NoHostAvailableException e)
        {
        }
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, null);
        logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0,((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
    }

    @Test
    public void testCqlPrepareQuerySyntaxError()
    {
        String cql = "INSERT INTO " + KEYSPACE + '.'+"foo"+ "(id, v1, v2) VALES (?,?,?)";
        try
        {
            createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

            Session session = sessionNet();

            PreparedStatement pstmt = session.prepare(cql);
            ResultSet rs = session.execute(pstmt.bind(1, "insert_audit", "test"));

        }
        catch (SyntaxError e)
        {
        }
        AuditLogEntry logEntry = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(logEntry, cql);
        assertEquals(0,((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
    }


    /**
     * Helper methods for Audit Log CQL Testing
     */

    private ResultSet executeAndAssert(String cql, AuditLogEntryType type) throws Throwable
    {
       return executeAndAssert(cql, type, false, KEYSPACE);
    }

    private ResultSet executeAndAssert(String cql, AuditLogEntryType type, boolean isTableNull, String keyspace) throws Throwable
    {
        Session session = sessionNet();

        ResultSet rs = session.execute(cql);

        AuditLogEntry logEntry1 = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cql, type, logEntry1, isTableNull, keyspace);

        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
        return rs;
    }
    private ResultSet executeAndAssertWithPrepare(String cql, AuditLogEntryType exceuteType, Object... bindValues) throws Throwable
    {
        return executeAndAssertWithPrepare(cql,exceuteType,false,bindValues);
    }
    private ResultSet executeAndAssertWithPrepare(String cql, AuditLogEntryType exceuteType, boolean isTableNull, Object... bindValues) throws Throwable
    {
        Session session = sessionNet();

        PreparedStatement pstmt = session.prepare(cql);
        ResultSet rs = session.execute(pstmt.bind(bindValues));

        AuditLogEntry logEntry1 = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cql, AuditLogEntryType.PREPARE_STATEMENT, logEntry1, isTableNull);

        AuditLogEntry logEntry2 = ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.poll();
        assertLogEntry(cql, exceuteType, logEntry2, isTableNull);

        assertEquals(0, ((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
        return rs;
    }

    private ResultSet executeAndAssertNoAuditLog(String cql,  Object... bindValues) throws Throwable
    {
        Session session = sessionNet();

        PreparedStatement pstmt = session.prepare(cql);
        ResultSet rs = session.execute(pstmt.bind(bindValues));

        assertEquals(0,((InMemoryAuditLogger) AuditLogManager.getInstance().getLogger()).inMemQueue.size());
        return rs;
    }


    private void assertLogEntry(String cql, AuditLogEntryType type, AuditLogEntry actual, boolean isTableNull)
    {
        assertLogEntry(cql,type,actual,isTableNull, KEYSPACE);
    }
    private void assertLogEntry(String cql, AuditLogEntryType type, AuditLogEntry actual, boolean isTableNull, String keyspace)
    {
       assertLogEntry(cql,currentTable(),type,actual,isTableNull,keyspace);

    }
    private void assertLogEntry(String cql, String table, AuditLogEntryType type, AuditLogEntry actual, boolean isTableNull, String keyspace)
    {
        assertEquals(keyspace, actual.getKeyspace());
        if(!isTableNull)
        {
            assertEquals(table, actual.getScope());
        }
        assertEquals(type, actual.getType());
        assertEquals(cql,actual.getOperation());

    }
    private void assertLogEntry(AuditLogEntry logEntry, String cql)
    {
        assertNull(logEntry.getKeyspace());
        assertNull(logEntry.getScope());
        assertEquals(AuditLogEntryType.REQUEST_FAILURE, logEntry.getType());
        if(null != cql && !cql.isEmpty())
        {
            assertThat(logEntry.getOperation(), containsString(cql));
        }
    }
}
