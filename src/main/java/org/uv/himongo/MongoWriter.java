package org.uv.himongo;

import java.io.IOException;

import org.apache.hadoop.mapred.*;
import org.bson.*;
import org.uv.himongo.io.BSONWritable;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

public class MongoWriter<K, V> implements RecordWriter<K, V> {
	MongoTable table;
	int _roundRobinCounter = 0;
    final int _numberOfHosts = 1;
    
	public MongoWriter(String host, String port, String dbName, String dbUser, String dbPasswd, String colName) {
		this.table = new MongoTable(host, port, dbName, dbUser, dbPasswd, colName);
	}

	public void close(boolean abort) throws IOException {
		if (table != null)
			table.close();
	}

	@Override
	public void write(K key, V value) throws IOException {
		final BasicDBObject o = new BasicDBObject();

        if ( key instanceof BSONWritable ){
            o.put("_id", ((BSONWritable)key).getDoc());
        }
        else if ( key instanceof BSONObject ){
            o.put( "_id", key );
        }
        else{
            o.put( "_id", BSONWritable.toBSON(key) );
        }

        if (value instanceof BSONWritable ){
            o.putAll( ((BSONWritable)value).getDoc() );
        }
        else if ( value instanceof BSONObject ){
            o.putAll( (BSONObject) value );
        }
        else{
            o.put( "value", BSONWritable.toBSON( value ) );
        }

        try {
        	this.table.save(o);
        } catch ( final MongoException e ) {
            e.printStackTrace();
            throw new IOException( "can't write to mongo", e );
        } 
	}
	
	@Override
	public void close(Reporter arg0) throws IOException {
		if (table != null)
			table.close();
	}

}
