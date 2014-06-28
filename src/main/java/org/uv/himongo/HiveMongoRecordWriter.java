package org.uv.himongo;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.uv.himongo.io.BSONWritable;

class HiveMongoRecordWriter extends MongoWriter<Object, BSONWritable> implements RecordWriter {

	public HiveMongoRecordWriter(String host, String port, String dbName, String dbUser, String dbPasswd, String colName) {
		super(host, port, dbName, dbUser, dbPasswd, colName);
	}
	
	@Override
	public void close(boolean abort) throws IOException {
		super.close(null);
	}
	
	@Override
	public void write(Writable w) throws IOException {
		super.write(null, (BSONWritable) w);
		
	}
}