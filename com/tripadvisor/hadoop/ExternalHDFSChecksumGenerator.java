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
 * This code was written by Josh Patterson.  Re-used with permission.  Please see:
 * https://github.com/jpatanooga/IvoryMonkey/blob/master/src/tv/floe/IvoryMonkey/hadoop/fs/ExternalHDFSChecksumGenerator.java
 * for the original code.
 * ------------------------------------------------------------
 */

package com.tripadvisor.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.DataChecksum;

/**
 *
 * This is a single threaded implementation of HDFS's checksumming mechanics
 * where it creates a MD5 hash of all of the MD5 block hashes of the CRC32's
 * that hdfs keeps for every 512 bytes it stores.
 *
 * In general HDFS keeps an extra 4 bytes as a CRC for each 512 bytes of block
 * data it stores.
 *
 * ToDo:
 *
 * - Document hadoop's execution path for calculating checksum code in hdfs
 * across machines in parallel
 *
 * Errata:
 *
 * - we discovered a bug in the implementation during the development of this
 * class for the openPDC - we filed the bug with AC Murthy and Tsz Wo of Yahoo:
 * http://issues.apache.org/jira/browse/HDFS-772 - the bug is described as"The fileMD5 is computed with the entire byte array returning by md5out.getData(). However, data are valid only up to md5out.getLength(). Therefore, the currently implementation of the algorithm compute fileMD5 with extra padding."
 * - currently this code works with CDH3b3 since the padding issue is consistent
 *
 * Usage
 *
 * - should be run from the Shell class - Shell class should be run as a Hadoop
 * jar to get proper config for talking to your cluster - this class can be very
 * handy in cases where a large file is moved multiple hops into hdfs and may
 * not directly use "hadoop fs -put" - in the case of the openPDC, we used a
 * customized FTP interface to talk to hdfs so we had a "2-hop" issue - we
 * pulled the checksum from hdfs and then used this code to calculate the
 * checksum on the client side to make sure we had the same file on both sides.
 *
 *
 *
 * @author jpatterson
 *
 */
public class ExternalHDFSChecksumGenerator extends Configured {

    protected FileSystem fs;

    public ExternalHDFSChecksumGenerator() {
        this(null);
    }

    public ExternalHDFSChecksumGenerator(Configuration conf) {
        super(conf);
        fs = null;
        // trash = null;
    }

    protected void init() throws IOException {

        if (getConf() == null) {
            System.out
                .println("ExternalHDFSChecksumGenerator > init > getConf() returns null!");
            return;
        }

        getConf().setQuietMode(true);
        if (this.fs == null) {
            this.fs = FileSystem.get(getConf());
        }

    }

    /**
     *
     * This is the function that calculates the hdfs-style checksum for a local file in the same way that
     * hdfs does it in a parallel fashion on all of the blocks in hdsf.
     *
     * @param strPath
     * @param bytesPerCRC
     * @param lBlockSize
     * @return
     * @throws IOException
     */
    public MD5MD5CRC32FileChecksum getLocalFilesystemHDFSStyleChecksum(String strPath, int bytesPerCRC, long lBlockSize)
        throws IOException
    {
        long lFileSize = 0;
        int iBlockCount = 0;
        DataOutputBuffer md5outDataBuffer = new DataOutputBuffer();
        DataChecksum chksm = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 512);
        InputStream in = null;
        MD5MD5CRC32FileChecksum returnChecksum = null;
        long crc_per_block = lBlockSize / bytesPerCRC;

        java.io.File file = new java.io.File(strPath);

        // FileStatus f_stats = srcFs.getFileStatus( srcPath );
        lFileSize = file.length();

        iBlockCount = (int) Math.ceil((double) lFileSize / (double) lBlockSize);

        // System.out.println( "Debug > getLen == " + f_stats.getLen() +
        // " bytes" );
        // System.out.println( "Debug > iBlockCount == " + iBlockCount );

        if (file.isDirectory())
        {
            throw new IOException("Cannot compute local hdfs hash, " + strPath
                                  + " is a directory! ");
        }

        try
        {
            in = new FileInputStream(file);
            long lTotalBytesRead = 0;

            for (int x = 0; x < iBlockCount; x++) {

                ByteArrayOutputStream ar_CRC_Bytes = new ByteArrayOutputStream();

                byte crc[] = new byte[4];
                byte buf[] = new byte[512];

                try {

                    int bytesRead = 0;

                    while ((bytesRead = in.read(buf)) > 0) {

                        lTotalBytesRead += bytesRead;

                        chksm.reset();
                        chksm.update(buf, 0, bytesRead);
                        chksm.writeValue(crc, 0, true);
                        ar_CRC_Bytes.write(crc);

                        if (lTotalBytesRead >= (x + 1) * lBlockSize) {

                            break;
                        }

                    } // while

                    DataInputStream inputStream = new DataInputStream(
                                                                      new ByteArrayInputStream(ar_CRC_Bytes.toByteArray()));

                    // this actually computes one ---- run on the server
                    // (DataXceiver) side
                    final MD5Hash md5_dataxceiver = MD5Hash.digest(inputStream);
                    md5_dataxceiver.write(md5outDataBuffer);

                } catch (IOException e) {

                    e.printStackTrace();

                } catch (Exception e) {

                    e.printStackTrace();

                }

            } // for

            // this is in 0.19.0 style with the extra padding bug
            final MD5Hash md5_of_md5 = MD5Hash.digest(md5outDataBuffer
                                                      .getData());
            returnChecksum = new MD5MD5CRC32FileChecksum(bytesPerCRC,
                                                         crc_per_block, md5_of_md5);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {

            e.printStackTrace();

        } finally {
            in.close();
        } // try

        return returnChecksum;

    }
}
