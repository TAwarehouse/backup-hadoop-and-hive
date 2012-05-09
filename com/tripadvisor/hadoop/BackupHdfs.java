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

public class BackupHdfs
{
    static private long m_nTotalBytes = 0;
    static private long m_nLastPercentBytesDone = 0;

    static private boolean m_bDryRun = false;

    private TablesToIgnore m_ignoreTables;
    private static int m_nIgnoredTables = 0;
    private PrintWriter m_wrMkdirs;
    private PrintWriter m_wrChmods;
    static private int m_nSleepSeconds;

    public BackupHdfs()
    {
        try
        {
            m_wrMkdirs = new PrintWriter(new BufferedWriter(new FileWriter("hdfs-mkdirs.sh")));
            m_wrMkdirs.println("hadoop fs -mkdir /mapred");
            m_wrMkdirs.println("hadoop fs -mkdir /mapred/system");

            m_wrChmods = new PrintWriter(new BufferedWriter(new FileWriter("hdfs-chmods.sh")));
            m_wrChmods.println("hadoop fs -chmod 775 /");
            m_wrChmods.println("hadoop fs -chown hdfs:hadoop /");
            m_wrChmods.println("hadoop fs -chown mapred:hadoop /mapred/system");
            m_wrChmods.println("hadoop fs -chmod 700 /mapred/system");
        }
        catch (IOException e)
        {
            System.err.println("ERROR: failed to open files for writing: "
                               + e.toString());
        }
    }

    // ------------------------------------------------------------

    /** closes all files
     *
     * @author tpalka@tripadvisor.com
     * @date   Thu Dec  1 22:00:05 2011
     */
    void closeFiles()
    {
        try
        {
            m_wrMkdirs.close();
            m_wrChmods.close();
        }
        catch (Exception e)
        {
            System.err.println("ERROR: failed to close files: "
                               + e.toString());
        }
    }

    /**
     * Prints out usage
     **/
    static void usage()
    {
        System.err.println("Usage: hadoop com.tripadvisor.hadoop.BackupHdfs args\n" +
                           "  --hdfs-path path/on/hdfs\n" +
                           "  --local-path path/on/local/fs: path to hdfs backup\n" +
                           "  --preserve-path path/on/local/fs: path to preserve old files\n" +
                           "  [--no-preserve FILE]: list of file substrings to skip preserving\n" +
                           "  [--ignore-tables FILE]: list of tables to ignore\n" +
                           "  [--dry-run]: don't create any files on local fs\n" +
                           "  --date yesterday|last-day|last-week|UNIX-time-T\n" +
                           "  [--max-date UNIX-time-T]: don't backup any files newer than T\n" +
                           "  [--sleep N]: sleep N seconds after each file copy\n" +
                           "  [--max-bytes N]: don't back up more than N bytes\n");

        System.exit(1);
    }

    public static void main(String[] args)
        throws IOException
    {
        Path baseDir = null;
        String localPath = null;
        String preservePath = null;
        String sIgnoreTablesFilename = null;
        String sNoPreserveFilename = null;
        String sDateString = null;
        long size = 0;

        // UNIX dates for right now
        long now = new java.util.Date().getTime() / 1000;
        long maxDate = now;

        for (int i=0 ; i<args.length ; i++)
        {
            if (args[i].equals("--hdfs-path"))
            {
                baseDir = new Path(args[++i]);
                continue;
            }
            if (args[i].equals("--local-path"))
            {
                localPath = args[++i];
                continue;
            }
            if (args[i].equals("--preserve-path"))
            {
                preservePath = args[++i];
                continue;
            }
            if (args[i].equals("--no-preserve"))
            {
                sNoPreserveFilename = args[++i];
                continue;
            }
            if (args[i].equals("--ignore-tables"))
            {
                sIgnoreTablesFilename = args[++i];
                continue;
            }
            if (args[i].equals("--sleep"))
            {
                try
                {
                    m_nSleepSeconds = Integer.parseInt(args[++i]);
                }
                catch (Exception e)
                {
                    System.err.println("ERROR: " + e.toString() + "\n");
                    usage();
                }
                continue;
            }
            if (args[i].equals("--dry-run"))
            {
                m_bDryRun = true;
                continue;
            }
            if (args[i].equals("--date"))
            {
                sDateString = args[++i];
                continue;
            }
            if (args[i].equals("--max-date"))
            {
                maxDate = Long.parseLong(args[++i]);
                continue;
            }
            if (args[i].equals("--max-bytes"))
            {
                size = Long.parseLong(args[++i]);
                continue;
            }

            System.err.println("ERROR: unknown arg " + args[i]);
            usage();
        }

        if (baseDir == null || localPath == null
            || preservePath == null
            || sDateString == null)
        {
            usage();
        }

        long minDate;

        if ("yesterday".equals(sDateString))
        {
            // figure out yesterday's dates
            Calendar cal = Calendar.getInstance();
            cal.roll(Calendar.DAY_OF_YEAR, -1);

            // yesterday midnight
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);

            minDate = cal.getTimeInMillis() / 1000;

            // yesterday end of day
            cal.set(Calendar.HOUR_OF_DAY, 23);
            cal.set(Calendar.MINUTE, 59);
            cal.set(Calendar.SECOND, 59);
            cal.set(Calendar.MILLISECOND, 999);

            maxDate = cal.getTimeInMillis() / 1000;
        }
        else if ("last-week".equals(sDateString))
        {
            minDate = maxDate - (7 * 24 * 60 * 60);
        }
        else if ("last-day".equals(sDateString))
        {
            minDate = maxDate - (24 * 60 * 60);
        }
        else
        {
            // UNIX date since epoch of last backup
            minDate = Long.parseLong(sDateString);
        }

        long tmpDate = 0;
        BackupHdfs bak = new BackupHdfs();

        // initialize the list of tables to ignore
        if (sIgnoreTablesFilename != null)
        {
            bak.initializeTablesToIgnore(sIgnoreTablesFilename);
        }

        // initialize list of files to not preserve
        if (sNoPreserveFilename != null)
        {
            bak.initializeNoPreserve(sNoPreserveFilename);
        }

        ArrayList<Path> pathList = new ArrayList<Path>(2000);
        HashMap<Path,Long> hmTimestamps = new HashMap<Path,Long>();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // If the HDFS path is a dir continue
        if (fs.getFileStatus(baseDir).isDir())
        {
            Calendar cal = Calendar.getInstance();

            System.err.println("");
            cal.setTimeInMillis(minDate * 1000);
            System.err.println("min date = " + cal.getTime().toString());

            cal.setTimeInMillis(maxDate * 1000);
            System.err.println("max date = " + cal.getTime().toString());

            System.err.println("");
            System.err.println("Searching filesystem: " +
                               baseDir.toUri().getPath());

            bak.checkDir(fs, minDate, maxDate, baseDir, pathList, hmTimestamps);

            System.err.println("");
            System.err.println("Skipped " + m_nIgnoredTables + " files due to ignored tables");

            System.err.println("");
            System.err.println("Number of files to backup = " +
                               pathList.size());

            System.err.println("Total bytes to backup = " + prettyPrintBytes(m_nTotalBytes));

            System.err.println("");
            System.err.println("sorting list of files...");
            Collections.sort(pathList, new DateComparator(hmTimestamps));
            System.err.println("done");

            System.err.println("");
            System.err.println("starting backup...");
            tmpDate = bak.backupFiles(localPath, preservePath, fs, pathList, size);

            bak.closeFiles();

            System.err.println("");
            System.err.println("backup completed...");
        }

        if (tmpDate == 0)
        {
            // If not size limit reached print out date for right now
            System.out.println(maxDate);
        }
        else
        {
            // Print out date for last file backed up
            System.err.println("Size limit reached.");
            System.out.println(tmpDate);
        }

        System.exit(0);
    }

    /**
     * Method to move files from HDFS to local filesystem
     *
     * localPath: Path on the machines filesystem
     * fs:FileSystem object from HDFS
     * pathList:List of paths for files that might need to be backed
     * up
     * size:max size in bytes to be backed up
     *
     * ReturnsDate of the last files backed up if reached size limit,
     * else, zero
     **/
    public long backupFiles(String localPath, String preservePath, FileSystem fs,
                            ArrayList<Path> pathList, long size) {
        Path fsPath;
        long tmpSize = 0;
        long tmpDate = 0;

        // Start iterating over all paths
        for (Path hdfsPath : pathList)
        {
            try
            {
                long nFileSize = fs.getContentSummary(hdfsPath).getLength();
                tmpSize = tmpSize + nFileSize;

                if ((tmpSize <= size) || (size == 0))
                {
                    FileStatus stat = fs.getFileStatus(hdfsPath);

                    System.err.println("File " + hdfsPath.toUri().getPath() +
                                       " " + nFileSize + " bytes, "
                                       + "perms: "
                                       + stat.getOwner() + "/" + stat.getGroup()
                                       + ", " + stat.getPermission().toString());

                    tmpDate = stat.getModificationTime() / 1000;

                    String sFsPath = localPath + hdfsPath.toUri().getPath();
                    fsPath = new Path(sFsPath);

                    File f = new File(sFsPath);

                    // COMMENTED OUT: until a few backup cycles run
                    // and the mtime gets in fact set on all copied
                    // files.
                    //
                    // ignore it if the file exists and has the same mtime
                    // if (f.exists() && f.isFile() && f.lastModified() == stat.getModificationTime())
                    // {
                    // System.out.println("no need to backup " + f.toString() + ", mtime matches hdfs");
                    // continue;
                    // }

                    if (false == m_bDryRun)
                    {
                        // check if we need to back up the local file
                        // (not directory), if it already exists.
                        if (f.exists() && f.isFile())
                        {
                            // ignore files with substrings in the
                            // no-preserve file
                            if (true == doPreserveFile(sFsPath))
                            {
                                // move it to the backup path
                                String sNewPath = preservePath + hdfsPath.toUri().getPath();
                                File newFile = new File(sNewPath);

                                // create directory structure for new file?
                                if (false == newFile.getParentFile().exists())
                                {
                                    if (false == newFile.getParentFile().mkdirs())
                                    {
                                        System.err.println("Failed to mkdirs " + newFile.getParentFile().toString());
                                        System.exit(1);
                                    }
                                }

                                // rename existing file to new location
                                if (false == f.renameTo(newFile))
                                {
                                    System.err.println("Failed to renameTo " + f.toString() + " to " + newFile.toString());
                                    System.exit(1);
                                }

                                System.out.println("preserved " + f.toString() + " into " + newFile.toString());
                            }
                            else
                            {
                                System.out.println("skipped preservation of " + f.toString());
                            }
                        }

                        // copy from hdfs to local filesystem
                        fs.copyToLocalFile(hdfsPath, fsPath);

                        // set the mtime to match hdfs file
                        f.setLastModified(stat.getModificationTime());

                        // compare checksums on both files
                        compareChecksums(fs, hdfsPath, sFsPath);
                    }

                    // don't print the progress after every file -- go
                    // by at least 1% increments
                    long nPercentDone = (long) (100 * tmpSize / m_nTotalBytes);
                    if (nPercentDone > m_nLastPercentBytesDone)
                    {
                        System.out.println("progress: copied " + prettyPrintBytes(tmpSize)
                                           + ", " + nPercentDone + "% done"
                                           + ", tstamp=" + tmpDate);

                        m_nLastPercentBytesDone = nPercentDone;
                    }

                    if (m_nSleepSeconds > 0)
                    {
                        try
                        {
                            Thread.sleep(1000 * m_nSleepSeconds);
                        }
                        catch (Exception e2)
                        {
                            // ignore
                        }
                    }
                }
                else
                {
                    return tmpDate;
                }
            }
            catch (IOException e)
            {
                System.err.println("FATAL ERROR: Something wrong with the file");
                System.err.println(e);
                System.out.println(tmpDate);
                System.exit(1);

                return 0;
            }
        }

        return 0;
    }

    /**
     * Method to go though the HDFS filesystem in a DFS to find all
     * files
     *
     * fs:FileSystem object from HDFS
     * minDate:      Oldest date for files to be backed up
     * maxDate:Newest date for files to be backed up
     * p:Path in HDFS to look for files
     * pathList:Will be filled with all files in p
     * hmTimestamps: hashmap of timestamps for later sorting
     **/
    public void checkDir(FileSystem fs, long minDate, long maxDate,
                         Path p, ArrayList<Path> pathList,
                         HashMap<Path,Long> hmTimestamps)
    {
        long tmpDate;
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

                // dump the mkdir and chmod commands for this
                // directory -- skip root directory only
                {
                    FileStatus stat = fs.getFileStatus(p);

                    if (! sPath.equals("/"))
                    {
                        m_wrMkdirs.println("hadoop fs -mkdir " + sPath);
                    }

                    m_wrChmods.println("hadoop fs -chown "
                                       + stat.getOwner() + ":"
                                       + stat.getGroup() + " "
                                       + sPath);

                    Short sh = new Short(stat.getPermission().toShort());
                    m_wrChmods.println("hadoop fs -chmod "
                                       + Long.toOctalString(sh.longValue())
                                       + " " + sPath);
                }


                fStat = fs.listStatus(p);

                // Do a recursive call to all elements
                for (int i = 0; i < fStat.length; i++)
                {
                    checkDir(fs, minDate, maxDate, fStat[i].getPath(),
                             pathList, hmTimestamps);
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
                // another database to regular hive tables to
                // partitioned hive tables.  We use table names to
                // both exclude some from the backup, and for the rest
                // to dump out the schema and partition name.
                if (m_ignoreTables != null && m_ignoreTables.doIgnoreFile(sPath))
                {
                    m_nIgnoredTables ++;

                    if (m_nIgnoredTables < 5)
                    {
                        System.out.println("Skipping ignore-table file: " + sPath);
                    }
                    else if (m_nIgnoredTables == 5)
                    {
                        System.out.println("(...not showing other skipped tables...)");
                    }
                    return;
                }

                FileStatus stat = fs.getFileStatus(p);

                tmpDate = stat.getModificationTime() / 1000;

                // store the chmods/chowns for all files
                m_wrChmods.println("hadoop fs -chown "
                                   + stat.getOwner() + ":"
                                   + stat.getGroup() + " "
                                   + sPath);

                m_wrChmods.println("hadoop fs -chmod "
                                   + stat.getPermission().toShort()
                                   + " " + sPath);

                // check dates.  is it too young?
                if (tmpDate < minDate)
                {
                    return;
                }

                // is the file too recent?
                if (tmpDate > maxDate)
                {
                    //System.out.println("file too recent: " + sPath);
                    return;
                }

                // file timestamp is ok
                pathList.add(p);

                hmTimestamps.put(p, new Long(tmpDate));

                // store info about total bytes neeed to backup
                m_nTotalBytes += fs.getContentSummary(p).getLength();
            }
        }
        catch (IOException e)
        {
            System.err.println("ERROR: could not open " + p + ": " + e);

            // System.exit(1) ;
        }
    }

    static class DateComparator implements Comparator
    {
        HashMap<Path,Long> hmTimestamps;

        public DateComparator(HashMap<Path,Long> hm)
        {
            hmTimestamps = hm;
        }

        public int compare(Object path1, Object path2)
        {
            long date1 = hmTimestamps.get((Path)path1).longValue();
            long date2 = hmTimestamps.get((Path)path2).longValue();

            if (date1 > date2) {
                return 1;
            } else if (date1 < date2) {
                return -1;
            } else {
                return 0;
            }
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

    private List<String> m_lNoPreserveSubstrings = null;

    // ------------------------------------------------------------

    /** read the file with filename substrings -- existing files with
     * those substrings will not get preserved to preserve-dir.
     *
     * @author tpalka@tripadvisor.com
     * @date   Thu Apr  5 17:43:37 2012
     */
    void initializeNoPreserve(String sFilename)
    {
        m_lNoPreserveSubstrings = new ArrayList<String>(10);

        BufferedReader in = null;

        try
        {
            in = new BufferedReader(new FileReader(sFilename));

            String s;
            while ((s = in.readLine()) != null)
            {
                s = s.trim();

                System.out.println("will not preserve files with substring: " + s);

                m_lNoPreserveSubstrings.add(s.trim());
            }
        }
        catch (Exception e)
        {
            System.out.println("ERROR: Failed to get no-preserve substrings: " + e);
            return;
        }
        finally
        {
            try { in.close(); } catch (Exception e2) {}
        }
    }

    // ------------------------------------------------------------

    /** returns true if sFilename should be preserved
     *
     * @author tpalka@tripadvisor.com
     * @date   Thu Apr  5 17:47:01 2012
     */
    boolean doPreserveFile(String sFilename)
    {
        if (null == m_lNoPreserveSubstrings)
        {
            return true;
        }

        int nLen = m_lNoPreserveSubstrings.size();
        for (int i=0 ; i<nLen ; i++)
        {
            String sSubstring = m_lNoPreserveSubstrings.get(i);
            if (sFilename.indexOf(sSubstring) > -1)
            {
                return false;
            }
        }

        return true;
    }


    // ------------------------------------------------------------

    /**  pretty print bytes
     *
     * @author tpalka@tripadvisor.com
     * @date   Tue Dec 20 14:51:47 2011
     */
    static DecimalFormat nf = new DecimalFormat("#,##0.##");

    static String prettyPrintBytes(double n)
    {
        if (n < 1) {
            return "-";
        }

        double nOrig = n;

        nf.setDecimalSeparatorAlwaysShown(true);

        final double ONE_K = 1024;

        if (n < ONE_K) {
            return nf.format(n) + " bytes";
        }

        n /= ONE_K;
        if (n < ONE_K) {
            return nf.format(n) + " KB";
        }

        n /= ONE_K;
        if (n < ONE_K) {
            return nf.format(n) + " MB";
        }

        n /= ONE_K;
        if (n < ONE_K) {
            return nf.format(n) + " GB";
        }

        n /= ONE_K;
        if (n < ONE_K) {
            return nf.format(n) + " TB";
        }

        return nf.format(nOrig) + " bytes";
    }

    // ------------------------------------------------------------

    // assume the hdfs block size.  Prod cluster uses 128M, as opposed
    // to the hadoop default 64M.
    final static long N_BLOCK_SIZE = 128 * 1024 * 1024;

    // ------------------------------------------------------------

    /** Compare the checksums of the hdfs file as well as the local
     * copied file.
     *
     * @author tpalka@tripadvisor.com
     * @date   Fri Jan 27 06:06:00 2012
     */
    boolean compareChecksums(FileSystem fs, Path p, String sFsPath)
    {
        try
        {
            // get hdfs file info
            FileStatus stat = fs.getFileStatus(p);

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

            // System.out.println(p.toUri().getPath() + " len=" + stat.getLen()
            // + " " + stat.getOwner() + "/" + stat.getGroup()
            // + " checksum=" + sCk);

            // find the local file
            File fLocal = new File(sFsPath);
            if (! fLocal.exists())
            {
                System.out.println("CHECKSUM-ERROR: file does not exist: " + sFsPath);
                return false;
            }
            if (! fLocal.isFile())
            {
                System.out.println("CHECKSUM-ERROR: path is not a file: " + sFsPath);
                return false;
            }
            if (stat.getLen() != fLocal.length())
            {
                System.out.println("CHECKSUM-ERROR: length mismatch: " + sFsPath
                                   + " hdfslen=" + stat.getLen()
                                   + " fslen=" + fLocal.length());
                return false;
            }

            // get local fs checksum
            FileChecksum ckLocal = getLocalFileChecksum(sFsPath);
            if (ckLocal == null)
            {
                System.out.println("ERROR Failed to get checksum for local file " + sFsPath);
                return false;
            }

            // compare checksums as a string, after stripping the
            // algorithm name from the beginning
            String sCkLocal = ckLocal.toString();
            String sCkLocalShort = sCkLocal.replaceAll("^.*:", "");

            if (false == sCkShort.equals(sCkLocalShort))
            {
                System.out.println("CHECKSUM-ERROR: checksum mismatch: " + sFsPath
                                   + "\nhdfs = " + sCk
                                   + "\nlocal= " + sCkLocal);
                return false;
            }

            return true;
        }
        catch (IOException e)
        {
            System.out.println("CHECKSUM-ERROR: " + sFsPath + " exception " + e.toString());
        }

        return false;
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
