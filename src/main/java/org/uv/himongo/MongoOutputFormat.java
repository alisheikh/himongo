package org.uv.himongo;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import static org.uv.himongo.ConfigurationUtil.*;

public class MongoOutputFormat<K, V> implements HiveOutputFormat<K, V>{

	@Override
	public RecordWriter getHiveRecordWriter(JobConf conf,
		      Path finalOutPath,
		      Class<? extends Writable> valueClass,
		      boolean isCompressed,
		      Properties tableProperties,
		      Progressable progress) throws IOException {
		return new HiveMongoRecordWriter(getDBHost(conf), getDBPort(conf), getDBName(conf), getDBUser(conf), getDBPassword(conf), getCollectionName(conf));
		
	}

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf conf)
			throws IOException {
	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}
}
