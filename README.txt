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

============================================================
OVERVIEW
============================================================

This project provides a method for backing up a hadoop cluster.  There
are two components that need to be backed up: the contents of the
hadoop filesystem (hdfs), and the hive DDL.  The latter is what
provides a way to string together the files kept in hdfs and access
them via HQL.

The BackupHdfs class deals with the first task.  It traverses the
entire hdfs filesystem, ordering all found files by timestamp.  It
then copies them (just like "hadoop fs -copyToLocal") to the local
filesystem.  Each file is checksum-verified after the copy to ensure
integrity.  BackupHdfs has options for ignoring files earlier than a
given timestamp, which is needed for incremental backups.  

BackupHdfs also dumps out two files that are used to restore the
directory structure and file permissions.  Those files are
hdfs-mkdirs.sh and hdfs-chmods.sh.

The DumpDDL class is used to access hive and dump out the HQL needed
to recreate the entire schema.  It dumps the table definitions,
including clustering, comments, and partitions.  The partition
definitions are what allow hive to correlate tables with the hdfs
files.

The run.sh script is a sample for how to invoke these classes inside
the hadoop and hive shells.  It is assumed that the user will want to
customize it for any particular needs.



============================================================
OPTIONS
============================================================

Please see each class for the complete list of command-line options.
The basic invocation of BackupHdfs is:

su hdfs -c "HADOOP_CLASSPATH=$HADOOP_CLASSPATH hadoop BackupHdfs
  --hdfs-path hdfs://MASTERNODE:56310/ --local-path /backup/current
  --preserve-path /backup/past/YYYYMMDD
  --ignore-tables ignore-tables.txt
  --date UNIX-TIME"

The above will backup hdfs contents that have been modified after
UNIX-TIME from hdfs://MASTERNODE:56310 to the local /backup/current
directory.  If a local file already exists, it will be moved to the
/backup/past/YYYYMMDD path.  The ignore-table.txt file can contain
names of tables that should be excluded from the backup.  Upon
completion, the BackupHdfs class will print out the modification time
of the latest file it backed up.  That time is suitable for use as
"--date" argument on the next invocation, for incremental backups.

The basic invocation of the DumpDDL class is:

hive --service jar backup-hadoop-and-hive.jar com.tripadvisor.hadoop.DumpDDL
  -D db.name=default 
  -D ignore.tables.filename=ignore-tables.txt
  >schema.ddl

Again, ignore-tables.txt has a list of table names to ignore.  db.name
is the name of the database.  The hive shell environment should
already be pointing to the hive database that should be dumped.


============================================================
RESTORING FROM BACKUP
============================================================

To restore from backup, use all the outputs in reverse.  In other words:

1. Create the directory hierarchy for hdfs:

    sudo -u hdfs hdfs-mkdirs.sh

2. Copy all the files back to hdfs, on the hdfs master node.  Example:

    cd /hdfs-backup/current
    sudo -u hdfs hadoop fs -copyFromLocal * MASTERNAME:56310/

3. Restore permissions

    sudo -u hdfs hdfs-chmods.sh

4. Re-create the schema

    Search and replace MASTERNAME to the correct machine in schema.ddl

    hive -f schema.ddl

    Note that because tables are created alphabetically, if view A
    depends on table Z, you will need to reorder, or just run the file
    twice.

To restore a file or a set of files to an older data point, re-run
step 2 from above from the appropriate
/disk1/hdfs-backup/past/YYYY-MM/DD directories. E.g. to restore the
entire cluster to a particular time, we'd copyFromLocal the files
backwards in time, from the most recent directory.

============================================================
CREDITS
============================================================

This work is based on three other open-sourced projects.  A big
thank-you to the authors who made this work available, they are:

* The HDFS-traversing code and file sorting code, basis for BackupHdfs,
  was written by Rapleaf.  Please see
  https://github.com/Rapleaf/HDFS-Backup

* The hdfs-style checksum of local files was written by Josh Patterson.  Please see
  https://github.com/jpatanooga/IvoryMonkey/blob/master/src/tv/floe/IvoryMonkey/hadoop/fs/ExternalHDFSChecksumGenerator.java

* The bulk of DumpDDL is based on Edward Capriolo's HiveShowCreateTable.  Please see
  https://issues.apache.org/jira/browse/HIVE-967
