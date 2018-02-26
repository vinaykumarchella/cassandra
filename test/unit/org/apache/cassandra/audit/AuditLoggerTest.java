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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;

/**
 * @author vchella
 */
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

    @AfterClass
    public static void tearDown()
    {
    }

    @Test
    public void testNoMissingCQLStatements() throws Throwable
    {
        List<Class<?>> res = findClassesImpmenenting(CQLStatement.class, CQLStatement.class.getPackage());
        List<Class<?>> missingClasses = new LinkedList<>();
        StringBuilder missingClassNames = new StringBuilder();
        for (Class<?> cl : res)
        {
            if( (! Modifier.isAbstract(cl.getModifiers())) && (! AuditLogEntryType.getAllStatementsMap().containsKey(cl.getCanonicalName())))
            {
                if(! AuditLogEntryType.getAllStatementsMap().containsKey(AuditLogEntryType.getParentClassName(cl.getTypeName())))
                {
                    missingClasses.add(cl);
                    missingClassNames.append(cl.getTypeName()).append(", ");
                }
            }

        }

        assertEquals("Following CQLStatements are missing in AuditLogEntryType enum. Please add these missing classes to AuditLogEntryType.allStatementsMap to fix this failing test. Missing classes : " + missingClassNames, 0, missingClasses.size());
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
        String indexName = createTableName();
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String tblName = currentTable();

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
        String triggerName = createTableName();

        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");

        String cql = "DROP TRIGGER IF EXISTS "+triggerName+ " ON "+KEYSPACE+"."+currentTable();
        executeAndAssert(cql, AuditLogEntryType.DROP_TRIGGER);

    }

    @Test
    public void testCqlAGGREGATEAuditing() throws Throwable
    {
        String aggName = createTableName();

        String cql = "DROP AGGREGATE IF EXISTS "+KEYSPACE+"."+aggName;
        executeAndAssert(cql, AuditLogEntryType.DROP_AGG);

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

    /**
     * Find All classes implementing an interface in a given package
     * @param interfaceClass
     * @param fromPackage
     * @return
     */
    public static List<Class<?>> findClassesImpmenenting(final Class<?> interfaceClass, final Package fromPackage)
    {

        final List<Class<?>> rVal = new ArrayList<Class<?>>();
        try
        {
            final Class<?>[] targets = getAllClassesFromPackage(fromPackage.getName());
            if (targets != null)
            {
                for (Class<?> aTarget : targets)
                {
                    if (aTarget == null)
                    {
                        continue;
                    }
                    else if (aTarget.equals(interfaceClass))
                    {
                        continue;
                    }
                    else if (!interfaceClass.isAssignableFrom(aTarget))
                    {
                        continue;
                    }
                    else
                    {
                        rVal.add(aTarget);
                    }
                }
            }
        }
        catch (ClassNotFoundException | IOException e)
        {

        }

        return rVal;
    }

    /**
     * Load all classes from a package.
     *
     * @param packageName
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public static Class[] getAllClassesFromPackage(final String packageName) throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class> classes = new ArrayList<Class>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes.toArray(new Class[classes.size()]);
    }

    /**
     * Find file in package.
     *
     * @param directory
     * @param packageName
     * @return
     * @throws ClassNotFoundException
     */
    public static List<Class<?>> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            }
            else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }
}
