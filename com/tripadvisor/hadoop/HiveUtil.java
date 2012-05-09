/*
  Copyright 2012 TripAdvisor, LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

/* ------------------------------------------------------------
 * This code is an addition to Edward Capriolo's HiveShowCreateTable
 * work.  Re-used and extended with permission.  Please see
 * https://issues.apache.org/jira/browse/HIVE-967 for Edward's
 * original code.
 * ------------------------------------------------------------
 */

package com.tripadvisor.hadoop;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.OptionsProcessor;

public class HiveUtil {

    public String hiveToMysqlType(String type){
        if (type.equalsIgnoreCase("string")){
            return " VARCHAR(255) ";
        }
        return type;
    }

    private String mysqlToHiveType(int type) {
        if (type == java.sql.Types.SMALLINT) {
            return "INT";
        }
        if (type == java.sql.Types.TINYINT) {
            return "INT";
        }
        if (type == java.sql.Types.INTEGER) {
            return "INT";
        }
        if (type == java.sql.Types.BIGINT) {
            return "INT";
        }
        if (type == java.sql.Types.DECIMAL) {
            return "DOUBLE";
        }
        if (type == java.sql.Types.DOUBLE) {
            return "DOUBLE";
        }
        if (type == java.sql.Types.FLOAT) {
            return "DOUBLE";
        }
        if (type == java.sql.Types.VARCHAR) {
            return "STRING";
        }
        if (type == java.sql.Types.CHAR) {
            return "STRING";
        }
        if (type == java.sql.Types.CLOB) {
            return "STRING";
        }

        throw new UnsupportedOperationException(
                                                "do not know what to do with sql.type " + type);
    }

    public List <FieldSchema> resultSetFieldList(String cmd, Configuration h2conf){

        int ret=0;
        SessionState.initHiveLog4j();
        SessionState ss = new SessionState(new HiveConf(SessionState.class));
        HiveConf conf = ss.getConf();
        SessionState.start(ss);
        String cmd_trimmed = cmd.trim();
        String[] tokens = cmd_trimmed.split("\\s+");
        String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
        CommandProcessor proc = null;
        try {
            proc = CommandProcessorFactory.get(tokens[0], (HiveConf) conf );
        } catch (NoSuchMethodError e){
            try {
                Class c = Class.forName("org.apache.hadoop.hive.ql.processors.CommandProcessorFactory");
                Method m = c.getMethod("get", new Class [] { String.class } );
                proc =(CommandProcessor) m.invoke(null, new Object[]{ tokens[0]});
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        if (proc instanceof Driver) {
            Driver driver = (Driver) proc;
            int res = driver.compile(cmd);
            Schema sch = driver.getSchema();
            List <FieldSchema> fields = sch.getFieldSchemas();
            return fields;
        } else {
            ret = proc.run(cmd_1).getResponseCode();
        }
        return null;
    }

    public static String join(List<? extends CharSequence> s, String delimiter) {
        int capacity = 0;
        int delimLength = delimiter.length();
        Iterator<? extends CharSequence> iter = s.iterator();
        if (iter.hasNext()) {
            capacity += iter.next().length() + delimLength;
        }

        StringBuilder buffer = new StringBuilder(capacity);
        iter = s.iterator();
        if (iter.hasNext()) {
            buffer.append(iter.next());
            while (iter.hasNext()) {
                buffer.append(delimiter);
                buffer.append(iter.next());
            }
        }
        return buffer.toString();
    }

    public int doHiveCommand(String cmd, Configuration h2conf ){
        int ret=0;
        SessionState.initHiveLog4j();
        SessionState ss = new SessionState(new HiveConf(SessionState.class));
        HiveConf conf = ss.getConf();
        SessionState.start(ss);

        String cmd_trimmed = cmd.trim();
        String[] tokens = cmd_trimmed.split("\\s+");
        String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
        //this is changed in trunk
        CommandProcessor proc = null;
        try {
            proc = CommandProcessorFactory.get(tokens[0], (HiveConf) conf );
        } catch (NoSuchMethodError e){
            try {
                Class c = Class.forName("org.apache.hadoop.hive.ql.processors.CommandProcessorFactory");
                Method m = c.getMethod("get", new Class [] { String.class } );
                proc =(CommandProcessor) m.invoke(null, new Object[]{ tokens[0]});
            } catch (Exception e1) {
                //many things could go wrong here
                e1.printStackTrace();
            }
        }
        if (proc instanceof Driver) {
            ret = proc.run(cmd).getResponseCode();
        } else {
            ret = proc.run(cmd_1).getResponseCode();
        }

        return ret;
    }

    public String getLocationForTable(String db, String table) {
        HiveConf hiveConf = new HiveConf(SessionState.class);
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        }
        org.apache.hadoop.hive.metastore.api.Table t = null;
        try {
            t = client.getTable(db, table);
        } catch (MetaException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        } catch (TException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        }
        StorageDescriptor sd = t.getSd();

        return anonymizeHostname(sd.getLocation());
    }

    public String showCreateTable(String db, String table){
        HiveConf hiveConf = new HiveConf(SessionState.class);
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        }
        org.apache.hadoop.hive.metastore.api.Table t = null;
        try {
            t = client.getTable(db, table);
        } catch (MetaException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        } catch (TException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        } catch (NoSuchObjectException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        }
        StorageDescriptor sd = t.getSd();
        StringBuilder results = new StringBuilder();
        results.append("-- table type: " + t.getTableType() + "\n\n");

        // special view handling
        if (t.getTableType().equalsIgnoreCase("VIRTUAL_VIEW"))
        {
            results.append("CREATE VIEW ");
            results.append(t.getTableName() + " AS \n");
            results.append(t.getViewOriginalText() + ";\n");

            return results.toString();
        }

        results.append("CREATE " );
        if (t.getTableType().equalsIgnoreCase("EXTERNAL_TABLE"))
        {
            results.append("EXTERNAL ");
        }

        results.append("TABLE IF NOT EXISTS ");
        results.append(t.getTableName() +" ( \n");

        //columns first
        List<FieldSchema> fsc = sd.getCols();
        List<String> columns = new ArrayList<String>();
        for (FieldSchema col: fsc){
	    String colName = col.getName();
	    if (colName.length() > 1 && colName.charAt(0) == '_') {
		colName = "`" + colName + "`";
	    }
            columns.add(" " +colName+" "+col.getType()+" "
                        + (col.isSetComment() ?
                           " COMMENT '" + col.getComment().replace("'", "\\'") + "'"
                           : ""));
        }
        results.append( HiveUtil.join(columns, ",\n"));
        results.append(" ) \n");

        // Comment, if present.  Escape any internal single quotes.
        // Single quotes do appear in comments; this is important.
        Map<String, String> tableparams = t.getParameters();
        if(tableparams.containsKey("comment")) {
            results.append(" COMMENT '"
                           +tableparams.get("comment").replace("'", "\\'")
                           +"'\n");
        }

        //partition columns
        List<FieldSchema> partKeys = t.getPartitionKeys();
        if (partKeys.size() > 0 ) {
            results.append(" PARTITIONED BY ( ");
            List<String> partCols = new ArrayList<String>();
            for (FieldSchema field: partKeys){
                partCols.add( field.getName() + " " + field.getType() ); // dont forget comments
            }
            results.append(HiveUtil.join(partCols, ",\n"));
            results.append(" ) \n");
        }

        //Clustering
        if (sd.isSetNumBuckets() && sd.getNumBuckets() > 1) {
            results.append("CLUSTERED BY (");
            results.append(HiveUtil.join(sd.getBucketCols(), ", "));
            results.append(") ");
            if (sd.isSetSortCols()) {
                List<String> sortcols = new ArrayList<String>();
                for(org.apache.hadoop.hive.metastore.api.Order o: sd.getSortCols()) {
                    sortcols.add(o.getCol() + " " + (o.getOrder() > 0 ? "ASC" : "DESC"));
                }
                // sortCols can be set but still zero.  Must NOT write "SORTED BY ()".
                if (!sortcols.isEmpty()) {
                    results.append("SORTED BY (");
                    results.append(HiveUtil.join(sortcols, ", "));
                    results.append(") ");
                }
            }
            results.append("INTO " + sd.getNumBuckets() + " BUCKETS\n");
        }

        //serde info
        SerDeInfo ser = sd.getSerdeInfo();
        results.append( " ROW FORMAT SERDE '"+ser.getSerializationLib()+"' \n" );
        if (ser.getParametersSize() > 0){
            results.append( "WITH SERDEPROPERTIES (");
            List<String> scolumns = new ArrayList<String>();
            for (Map.Entry<String,String> entry: ser.getParameters().entrySet() )
            {
                if (entry.getKey().equals("field.delim")) {
                    scolumns.add( " '"+entry.getKey()+"'="+charToEscapedOctal(entry.getValue().charAt(0)) );
                } else if (entry.getKey().equals("line.delim")) {
                    scolumns.add( " '"+entry.getKey()+"'="+charToEscapedOctal(entry.getValue().charAt(0)) );
                }  else if (entry.getKey().equals("serialization.format")) {
                    scolumns.add( " '"+entry.getKey()+"'="+charToEscapedOctal(entry.getValue().charAt(0)) );
                } else {
                    scolumns.add( " '"+entry.getKey()+"'='"+entry.getValue()+"'" );
                }
            }
            results.append( HiveUtil.join(scolumns, ",\n"));
            results.append(" ) ");
        }
        results.append( " STORED AS INPUTFORMAT '"+sd.getInputFormat()+"' \n");
        results.append( " OUTPUTFORMAT '"+sd.getOutputFormat()+"' \n" );

        String sLocation = anonymizeHostname(sd.getLocation());

        results.append( " LOCATION '"+sLocation+"'\n" );

        results.append(";\n");
        return results.toString();
    }

    public String charToEscapedOctal(char c){
        return "'\\"+ StringUtils.leftPad( Integer.toOctalString( c),3,'0') +"'";
    }

    // ------------------------------------------------------------

    /** fixup the location hostname -- to prevent copy & paste errors,
     * and to force the restorer to use the right master hostname
     *
     * @author tpalka@tripadvisor.com
     * @date   Fri Dec  9 12:37:02 2011
     */
    private static final Pattern P_HOSTNAME =
        Pattern.compile("hdfs://[^:]+:(\\d+.*)$");

    String anonymizeHostname(String sLocation)
    {
        if (sLocation == null)
        {
            return null;
        }

        Matcher m = P_HOSTNAME.matcher(sLocation);
        if (m.find())
        {
            return "hdfs://MASTERHOSTNAME:" + m.group(1);
        }

        return sLocation;
    }

    // ------------------------------------------------------------

    private static final Pattern P_PARTITION =
        Pattern.compile("/*([^=]+)=([^/]+)");

    /** following the example of showCreateTable, this one will dump
     * the "ADD PARTITION" commands for all partitions
     *
     * @author tpalka@tripadvisor.com
     * @date   Wed Nov 23 14:30:36 2011
     */
    public String showAddPartitions(String db, String table)
    {
        HiveConf hiveConf = new HiveConf(SessionState.class);
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        }

        // first get the partition location, this is the hdfs:// path
        // to where the table is on disk
        String sLocation = getLocationForTable(db, table);

        List<String> lPartitionNames = null;

        try
        {
            lPartitionNames = client.listPartitionNames(db, table, java.lang.Short.MAX_VALUE);
        }
        catch (MetaException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        } catch (TException e) {
            throw new RuntimeException("getting location for " + db + " "
                                       + table, e);
        }

        StringBuilder sb = new StringBuilder();

        if (lPartitionNames != null)
        {
            Iterator<String> iter = lPartitionNames.iterator();
            while (iter.hasNext())
            {
                String sName = iter.next();

                // sName will be in format ds=2011-11-11, fix it up to
                // HQL ds='2011-11-11'.  Sometimes we have multiple
                // partition columns, then they appear as:
                // ds=2011-04-16/request_started_hour=3
                Matcher m = P_PARTITION.matcher(sName);
                String sPrettyName = "";
                while (m.find())
                {
                    if (sPrettyName.length() > 0)
                    {
                        sPrettyName += ",";
                    }
                    sPrettyName += m.group(1) + "='" + m.group(2) + "'";
                }

                // ALTER TABLE table_name ADD [IF NOT EXISTS] PARTITION partition_spec [LOCATION 'location1'] ...
                sb.append("\nALTER TABLE ").append(table);
                sb.append(" ADD IF NOT EXISTS PARTITION(");
                sb.append(sPrettyName);
                sb.append(") LOCATION '").append(sLocation).append("/").append(sName).append("';");
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    // ------------------------------------------------------------

    /** get a list of all tables, and then dump their schema and partitions
     *
     * @author tpalka@tripadvisor.com
     * @date   Wed Nov 23 14:51:16 2011
     */
    public String dumpDDL(String db, String sFilenameIgnoreTables)
    {
        HiveConf hiveConf = new HiveConf(SessionState.class);
        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException("dumping tables for " + db, e);
        }

        List<String> lTables = null;
        try
        {
            // lTables = client.getTables(db, "*");
            lTables = client.getAllTables(db);
        }
        catch (MetaException e) {
            throw new RuntimeException("dump tables for " + db, e);
        }

        // get the list of tables to ignore
        TablesToIgnore ignoreTables = null;
        if (sFilenameIgnoreTables != null)
        {
            ignoreTables = new TablesToIgnore(sFilenameIgnoreTables);
        }

        if (lTables != null)
        {
            Iterator<String> iter = lTables.iterator();
            while (iter.hasNext())
            {
                String sTable = iter.next();
                System.out.println("-- found table: " + sTable);

                if (ignoreTables != null
                    && ignoreTables.doIgnoreTable(sTable))
                {
                    // skip
                    System.out.println("-- ignoring: " + sTable);
                    continue;
                }

                // our ignored tables often have a _YYYYMMDD at th
                // end, so also check to see if that should be
                // ignored.
                String sTableStripped = sTable.replaceAll("_20[0-9][0-9][01][0-9][0-3][0-9]$", "");

                if (ignoreTables != null
                    && ignoreTables.doIgnoreTable(sTableStripped.toLowerCase()))
                {
                    // skip
                    System.out.println("-- ignoring: " + sTable + ", base name is " + sTableStripped);
                    continue;
                }

                StringBuilder sb = new StringBuilder();

                sb.append(showCreateTable(db, sTable));
                sb.append("\n\n");

                sb.append(showAddPartitions(db, sTable));
                sb.append("\n\n");

                System.out.println(sb.toString());
            }
        }

        return "";
    }
}
