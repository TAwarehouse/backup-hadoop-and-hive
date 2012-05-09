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
 * The HDFS-traversing code and file sorting code was written by
 * Rapleaf.  Re-used and adapted with permission.  Please see:
 * https://github.com/Rapleaf/HDFS-Backup
 * for the original code.
 * ------------------------------------------------------------
 */
package com.tripadvisor.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.*;
import java.sql.*;

/** Traverse hdfs filesystem, ignoring the same files that are ignored
 * by the BackupHdfs class.  But process all files, without any
 * concern for their age.  Compare their checksum / date / size
 * against the files on the local backup filesystem.
 *
 * @author tpalka@tripadvisor.com
 * @date   Wed Jan  4 08:41:23 2012
 */

public class VerifyHdfsBackup
{
    private TablesToIgnore m_ignoreTables;

    // assume the hdfs block size.  Prod cluster uses 128M, as opposed
    // to the hadoop default 64M.
    final static long N_BLOCK_SIZE = 128 * 1024 * 1024;

    public VerifyHdfsBackup()
    {
    }

    /**
     * Prints out usage
     */
    static void usage()
    {
        System.err.println("Usage: hadoop com.tripadvisor.hadoop.VerifyHdfsBackup args\n" +
                           "  --hdfs-path path/on/hdfs\n" +
                           "  --local-path path/on/local/fs: path to hdfs backup\n" +
                           "  [--max-date UNIX-time]: don't verify any newer files\n" +
                           "  [--ignore-tables FILE]: list of tables to ignore\n" +
                           "  --from-file FILE: list of filenames to verify\n");


        System.exit(1);
    }

    public static void main(String[] args)
        throws IOException
    {
        Path baseDir = null;
        String sLocalPathRoot = null;
        String sIgnoreTablesFilename = null;
        String sMaxDateString = null;
        String sFromFilename = null;

        for (int i=0 ; i<args.length ; i++)
        {
            if (args[i].equals("--hdfs-path"))
            {
                baseDir = new Path(args[++i]);
                continue;
            }
            if (args[i].equals("--local-path"))
            {
                sLocalPathRoot = args[++i];
                continue;
            }
            if (args[i].equals("--ignore-tables"))
            {
                sIgnoreTablesFilename = args[++i];
                continue;
            }
            if (args[i].equals("--max-date"))
            {
                sMaxDateString = args[++i];
                continue;
            }
            if (args[i].equals("--from-file"))
            {
                sFromFilename = args[++i];
                continue;
            }

            System.err.println("ERROR: unknown arg " + args[i]);
            usage();
        }

        if (baseDir == null || sLocalPathRoot == null)
        {
            usage();
        }

        // UNIX date for right now
        long maxDate = new java.util.Date().getTime() / 1000;

        if (sMaxDateString != null)
        {
            // UNIX date since epoch of last backup
            maxDate = Long.parseLong(sMaxDateString);
        }

        VerifyHdfsBackup bak = new VerifyHdfsBackup();

        // initialize the list of tables to ignore
        if (sIgnoreTablesFilename != null)
        {
            bak.initializeTablesToIgnore(sIgnoreTablesFilename);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (sFromFilename != null)
        {
            BufferedReader in = null;
            try
            {
                in = new BufferedReader(new FileReader(sFromFilename));
                String sFile;
                while ((sFile = in.readLine()) != null)
                {
                    bak.checkDir(fs, new Path(sFile), sLocalPathRoot, maxDate);
                }
            }
            catch (Exception e)
            {
                System.out.println("ERROR: Failed to read from-file " + sFromFilename + ": " + e);
            }
            finally
            {
                try { in.close(); } catch (Exception e2) {}
            }
        }
        else
        {
            // If the HDFS path is a dir continue
            if (fs.getFileStatus(baseDir).isDir())
            {
                System.out.println("Searching filesystem: " +
                                   baseDir.toUri().getPath());

                bak.checkDir(fs, baseDir, sLocalPathRoot, maxDate);
            }
        }

        System.exit(0);
    }

    /**
     * Method to go though the HDFS filesystem in a DFS to find all
     * files
     *
     * fs:FileSystem object from HDFS
     * maxDate:Newest date for files to be backed up
     * p:Path in HDFS to look for files
     **/
    public void checkDir(FileSystem fs, Path p, String sLocalPathRoot, long maxDate)
    {
        FileStatus[] fStat;

        try
        {
            String sPath = p.toUri().getPath();

            // If this is a directory
            if (fs.getFileStatus(p).isDir())
            {
                // ignore certain directories
                if ("dfstmp".equals(p.getName())
                    || "tmp".equals(p.getName())
                    || "jobtracker".equals(p.getName())
                    || sPath.startsWith("/mapred")
                    || "ops".equals(p.getName())
                    || p.getName().startsWith("_distcp_logs")
                    )
                {
                    return;
                }

                fStat = fs.listStatus(p);

                // Do a recursive call to all elements
                for (int i = 0; i < fStat.length; i++)
                {
                    checkDir(fs, fStat[i].getPath(), sLocalPathRoot, maxDate);
                }
            }
            else
            {
                // If not a directory then we've found a file

                // ignore crc files
                if (p.getName().endsWith(".crc"))
                {
                    return;
                }

                // ignore other files
                if (sPath.startsWith("/user/oozie/etl/workflows/"))
                {
                    return;
                }

                // try to get the table name from the path. There are
                // various types of tables, from those replicated from
                // tripmonster to regular hive tables to partitioned
                // hive tables.  We use table names to both exclude
                // some from the backup, and for the rest to dump out
                // the schema and partition name.
                if (m_ignoreTables != null
                    && m_ignoreTables.doIgnoreFile(sPath))
                {
                    return;
                }

                // check the file
                FileStatus stat = fs.getFileStatus(p);

                // ignore files that are too new
                if ((stat.getModificationTime() / 1000) > maxDate)
                {
                    System.out.println("IGNORING: " + sPath + " too new");
                    return;
                }

                // warn about files that have a mis-matching block
                // size.  The checksum check will fail for them
                // anyways, so just catch it here.
                if (stat.getBlockSize() != N_BLOCK_SIZE)
                {
                    System.out.println("ERROR: non-default block size (" +
                                       (stat.getBlockSize() / (1024*1024))
                                       + "M) would fail checksum: " + sPath);
                    return;
                }

                // get HDFS checksum
                FileChecksum ck = fs.getFileChecksum(p);
                String sCk, sCkShort;
                if (ck == null)
                {
                    sCk = sCkShort = "<null>";
                }
                else
                {
                    sCk = ck.toString();
                    sCkShort = sCk.replaceAll("^.*:", "");
                }

                System.out.println(sPath + " len=" + stat.getLen()
                                   + " " + stat.getOwner() + "/" + stat.getGroup()
                                   + " checksum=" + sCk);

                // find the local file
                String sFsPath = sLocalPathRoot + p.toUri().getPath();
                File fLocal = new File(sFsPath);
                if (! fLocal.exists())
                {
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(stat.getModificationTime());

                    System.out.println("ERROR: file does not exist: " + sFsPath
                                       + " hdfs-last-mtime=" + cal.getTime().toString());
                    return;
                }
                if (! fLocal.isFile())
                {
                    System.out.println("ERROR: path is not a file: " + sFsPath);
                    return;
                }
                if (stat.getLen() != fLocal.length())
                {
                    System.out.println("ERROR: length mismatch: " + sFsPath
                                       + " hdfslen=" + stat.getLen()
                                       + " fslen=" + fLocal.length());
                    return;
                }

                // get local fs checksum
                FileChecksum ckLocal = getLocalFileChecksum(sFsPath);
                if (ckLocal == null)
                {
                    System.out.println("ERROR Failed to get checksum for local file " + sFsPath);
                    return;
                }

                // compare checksums as a string, to strip the
                // algorithm name from the beginning
                String sCkLocal = ckLocal.toString();
                String sCkLocalShort = sCkLocal.replaceAll("^.*:", "");

                if (false == sCkShort.equals(sCkLocalShort))
                {
                    System.out.println("ERROR: checksum mismatch: " + sFsPath
                                       + "\nhdfs = " + sCk
                                       + "\nlocal= " + sCkLocal);
                    return;
                }
            }
        }
        catch (IOException e)
        {
            System.out.println("ERROR: could not open " + p + ": " + e);

            // System.exit(1) ;
        }
    }

    // ------------------------------------------------------------

    /** get the list of tables that get synced from tripmaster --
     * we'll want to ignore those.  Stores the names in lowercase in
     * the provided hashset.
     *
     * @author tpalka@tripadvisor.com
     * @date   Tue Nov 22 16:57:42 2011
     */
    void initializeTablesToIgnore(String sFilename)
    {
        m_ignoreTables = new TablesToIgnore(sFilename);
    }

    // ------------------------------------------------------------

    static ExternalHDFSChecksumGenerator g_checksumGenerator;

    // ------------------------------------------------------------

    /**
     *
     * @author tpalka@tripadvisor.com
     * @date   Sat Jan  7 05:51:47 2012
     */
    MD5MD5CRC32FileChecksum getLocalFileChecksum(String sPath)
    {
        if (g_checksumGenerator == null)
        {
            g_checksumGenerator = new ExternalHDFSChecksumGenerator();
        }

        // copied from checksum generator code
        long lBlockSize = N_BLOCK_SIZE;
        int bytesPerCRC = 512;

        try
        {
            return g_checksumGenerator.getLocalFilesystemHDFSStyleChecksum
                (sPath, bytesPerCRC, lBlockSize);
        }
        catch (Exception e)
        {
            System.out.println("ERROR getting local checksum: " + e.toString());
            e.printStackTrace();
            return null;
        }
    }
}
