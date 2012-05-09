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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DumpDDL extends Configured implements Tool {

    public static String DB_NAME="db.name";

    @Override
        public int run(String[] args) throws Exception {
        Configuration config = getConf();
        JobConf conf = new JobConf(config,DumpDDL.class);

        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        for (String arg: args){
            if (arg.contains("=")){
                String vname = arg.substring(0,arg.indexOf('='));
                String vval = arg.substring(arg.indexOf('=')+1);
                conf.set( vname,vval.replace("\"", "") );
            }
        }

        HiveUtil hu  = new HiveUtil();
        System.out.println(hu.dumpDDL(conf.get(DB_NAME, "default"),
                                      conf.get("ignore.tables.filename")));

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new DumpDDL(), args);
        System.exit(ret);
    }

}
