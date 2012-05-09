#!/bin/bash
# 
#   Copyright 2012 TripAdvisor, LLC
# 
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# @author tpalka@tripadvisor.com
# @purpose invoke the BackupHdfs and DumpDDL classes 
#

HDFSMASTER="hdfs://hadoopmaster.domain.com:56310/"

# $topDir will refer to the top location for the hdfs backup.  Under it
# will be hdfs-backup and backup-logs subdirectories, as well as
# the lock and timestamp files
topDir="/disk1"

# lock file support to prevent two processes from running at the same time
lockFile="$topDir/BACKUP-LOCK"
if [[ -f $lockFile ]];
then
    echo "ERROR: found lock file $lockFile"
    exit 1
fi
date > $lockFile

INSTALLDIR=/path/to/backupHadoopAndHive

tstampFile=$topDir/hdfs-backup/LAST_TSTAMP
if [[ ! -f $tstampFile ]]; 
then
    echo 0 > $tstampFile
fi

# make sure that file has a valid unix date in it
cnt="`grep -c -E '^[0-9]+$' $tstampFile`"
if [[ $cnt != 1 ]];
then
    echo "ERROR: no valid last time found in $tstampFile"
    exit 1
fi

minDateArg="--date `cat $tstampFile`"

# never backup files younger than yesterday midnight.  Get yesterday's 
# date in YYYY-MM-DD format, then convert it to epoch
yesterday="`date --date='1 days ago' +%Y-%m-%d`"
yesterdayEpoch=`date --date=$yesterday +%s`
maxDateArg="--max-date $yesterdayEpoch"

# TESTING HACK!  Uncomment to run the backup for a particular
# date range:
# # date -d 2012-02-07 +%s
# minDateArg="--date 1328590800"
# # date -d 2012-02-08 +%s
# maxDateArg="--max-date 1328677200"


odir="$topDir/backup-logs/`date +%Y%m%d`"
if [[ -d $odir ]]
then
    rm -f $odir.4 >/dev/null 2>&1 
    mv -f $odir.3 $odir.4 >/dev/null 2>&1
    mv -f $odir.2 $odir.3 >/dev/null 2>&1
    mv -f $odir.1 $odir.2 >/dev/null 2>&1
    mv -f $odir   $odir.1 >/dev/null 2>&1
fi
mkdir -p $odir
chmod 777 $odir

cd $odir

echo "outputs in $odir"
echo ""

CP=$INSTALLDIR/backup-hadoop-and-hive.jar


#----------------------------------------
# get the list of tables to ignore
#----------------------------------------

# generate your ignore tables here, be it by running another program or
# just echo'ing into the file.  Tables which contain the listed name
# plus a suffix of _YEAR* will also be ignored; if this is not what
# you want, edit TablesToIgnore.java.
rm -f ignore-tables.txt
echo "sample_table"   >>ignore-tables.txt

#----------------------------------------
# backup HDFS
#----------------------------------------

# directory where the full dump of hdfs lives
backupDir="$topDir/hdfs-backup/current"
if [[ ! -d $backupDir ]]
then
    mkdir -p $backupDir
    chmod 755 $backupDir
    chown -R hdfs $backupDir
fi

# directory where preserved files (files that existed in the 
# full dump and would get overwritten) are moved to
preserveDir="$topDir/hdfs-backup/past/`date +%Y-%m`/`date +%d`"
if [[ ! -d $preserveDir ]]
then
    mkdir -p $preserveDir
    chmod 755 $preserveDir
    chown -R hdfs $preserveDir
fi

# run the java app in hadoop process
echo;echo -n "starting hdfs backup: "; date
su hdfs -c "HADOOP_CLASSPATH=$CP hadoop com.tripadvisor.hadoop.BackupHdfs --hdfs-path $HDFSMASTER --local-path $backupDir --preserve-path $preserveDir --ignore-tables ignore-tables.txt $minDateArg $maxDateArg $dryRunArg" >backup.log 2>&1
echo;echo -n "done with hdfs backup: "; date

if [[ ! $? ]]; 
then
    echo;echo;echo
    echo "ERROR: backup failed!"
    tail -n 50 backup.log
else
    rm -f $tstampFile".prev" >/dev/null 2>&1
    mv -f $tstampFile $tstampFile".prev" >/dev/null 2>&1
    tail -n 1 backup.log > $tstampFile

    # show some of the output, summarize preserved files
    echo;echo "--------------------"
    grep -v "File /.* bytes, perms: " backup.log \
        | grep -v "^progress" \
        | sed 's/ into.*//;' \
        | sed -re 's/\/[0-9]+_[0-9]//g;' \
        | sed -re 's/[a-z_]+=[^\/ ]+//g;' \
	| uniq -c \
        | sed 's/^      1 /        /;'
    echo;echo "--------------------"
fi

#----------------------------------------
# dump the Hive schema
#----------------------------------------
date
echo;echo "dumping schema into $odir/schema.ddl: "
rm -f schema.ddl
time (hive --service jar $INSTALLDIR/backup-hadoop-and-hive.jar com.tripadvisor.hadoop.DumpDDL -D db.name=default -D ignore.tables.filename=ignore-tables.txt >schema.ddl) >dumpddl.log 2>&1

echo "exit status: $?"

echo;echo -n "done with schema dump: "; date

rm -f $lockFile

exit 0

