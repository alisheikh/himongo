package org.uv.himongo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.bson.BSONObject;
import org.uv.himongo.io.BSONWritable;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoReader implements RecordReader<LongWritable, BSONWritable> {
	final static int BATCH_SIZE = 8192;
	MongoTable table;
	MongoSplit split;
	DBCursor cursor;
	long pos;
	String[] readColumns;
	BSONObject current;

	public MongoReader(String dbHost, String dbPort, String dbName, String dbUser, String dbPasswd, 
			String colName, MongoSplit split, String[] readColumns) {
		this.table = new MongoTable(dbHost, dbPort, dbName, dbUser, dbPasswd, colName);
		this.split = split;
		this.readColumns = readColumns;

		this.cursor = table.findAll(readColumns).batchSize(BATCH_SIZE).skip(
				(int) split.getStart());
		if (!split.isLastSplit())
			this.cursor.limit((int) split.getLength());// if it's the last
		// split,it will read
		// all records since
		// $start

	}

	@Override
	public void close() throws IOException {
		if (table != null)
			table.close();
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public BSONWritable createValue() {
		return new BSONWritable();
	}

	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public float getProgress() throws IOException {
		return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
	}

	//TODO
	@Override
	public boolean next(LongWritable keyHolder, BSONWritable valueHolder)
			throws IOException {
		if (!cursor.hasNext()) {
			return false;
		}
		current = cursor.next();
		valueHolder.setDoc(current);
		keyHolder.set(pos);
		/*for (int i = 0; i < this.readColumns.length; i++) {
			String key = readColumns[i];
			Object vObj = ("id".equals(key)) ? record.get("_id") : record
					.get(key);
			Writable value = (vObj == null) ? NullWritable.get() : new Text(
					vObj.toString());
			valueHolder.put(new Text(key), value);
		}*/
		pos++;
		return true;
	}

}
