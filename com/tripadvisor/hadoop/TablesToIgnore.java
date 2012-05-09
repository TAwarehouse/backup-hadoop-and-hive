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

package com.tripadvisor.hadoop;

import java.util.*;
import java.util.regex.*;
import java.io.*;

/** helper class.  manages the list of tables that we should exclude.
 * does some rudimentary matching against filenames to try to figure
 * out which table they represent.
 *
 * @author tpalka@tripadvisor.com
 * @date   Fri Feb 10 17:10:21 2012
 */
final class TablesToIgnore
{
    private static final Pattern P_TABLENAME =
        Pattern.compile("/user/hive/warehouse/([^\\/]+)_20[0-9]+");

    private static final Pattern P_TABLENAME2 =
        Pattern.compile("/user/hive/warehouse/([^\\/]+)/");

    private HashSet<String> m_hs;

    // ------------------------------------------------------------

    /** constructor.  Requires name of file to parse, format is one
     * table name per line.
     *
     * @author tpalka@tripadvisor.com
     * @date   Fri Feb 10 17:11:34 2012
     */
    TablesToIgnore(String sFilename)
    {
        m_hs = initializeFromFile(sFilename);
    }

    // ------------------------------------------------------------

    /** apply a set of regex patterns to get the table name from a
     * given filepath.  Used to match an hdfs file with a hive table.
     *
     * @author tpalka@tripadvisor.com
     * @date   Wed Nov 23 12:57:57 2011
     */
    String tryToGetTableName(String sFile)
    {
        if (sFile == null || false == sFile.startsWith("/user/hive"))
        {
            return null;
        }

        Matcher m;

        m = P_TABLENAME.matcher(sFile);
        if (m.find())
        {
            return m.group(1).toLowerCase();
        }

        m = P_TABLENAME2.matcher(sFile);
        if (m.find())
        {
            return m.group(1).toLowerCase();
        }

        return null;
    }

    // ------------------------------------------------------------

    /** parses a text file to get list of tables to ignore
     *
     * @author tpalka@tripadvisor.com
     * @date   Fri Feb 10 17:12:31 2012
     */
    private HashSet<String> initializeFromFile(String sFilename)
    {
        HashSet<String> hs = new HashSet<String>();

        BufferedReader in = null;

        try
        {
            in = new BufferedReader(new FileReader(sFilename));

            String sTable;
            while ((sTable = in.readLine()) != null)
            {
                sTable = sTable.trim().toLowerCase();

                hs.add(sTable);
            }
        }
        catch (Exception e)
        {
            System.out.println("ERROR: Failed to get ignored-tables: " + e);
            return null;
        }
        finally
        {
            try { in.close(); } catch (Exception e2) {}
        }

        System.out.println("-- will ignore " + hs.size() + " tables");

        return hs;
    }

    // ------------------------------------------------------------

    /** tries to match the file to a table, and then check if the
     * table should be ignored.
     *
     * @author tpalka@tripadvisor.com
     * @date   Fri Feb 10 17:17:02 2012
     */
    boolean doIgnoreFile(String sPath)
    {
        String sTable = tryToGetTableName(sPath);

        // ignore files that are for ignored tables, e.g.
        // /user/hive/warehouse/t_postalcodes
        return (sTable != null && m_hs.contains(sTable.toLowerCase()));
    }

    // ------------------------------------------------------------

    /** returns true if the table should be ignored
     *
     * @author tpalka@tripadvisor.com
     * @date   Fri Feb 10 17:26:54 2012
     */
    boolean doIgnoreTable(String sTable)
    {
        return (sTable != null && m_hs.contains(sTable.toLowerCase()));
    }


}