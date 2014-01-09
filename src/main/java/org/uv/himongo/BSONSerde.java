package org.uv.himongo;

/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.uv.himongo.io.BSONWritable;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;

public class BSONSerde implements SerDe {

    private static final Log LOG = LogFactory.getLog(BSONSerde.class.getName());

    /**
     * # of columns in the Hive table
     */
    private int numColumns;
    /**
     * Column names in the Hive table
     */
    private List<String> columnNames;
    /**
     * An ObjectInspector which contains metadata
     * about rows
     */
    private StructObjectInspector rowInspect;

    private ArrayList<Object> row;

    private List<TypeInfo> columnTypes;

    private long serializedSize;
    private SerDeStats stats;
    private boolean lastOperationSerialize;
    private boolean lastOperationDeserialize;

    private boolean test = false;
    public Map<String, String> hiveToMongo;
    
    private static final int BSON_NUM = 8;
    private static final String OID = "oid";
    private static final String BSON_TYPE = "bsontype";

    public void initialize(Configuration sysProps, Properties tblProps)
                                                throws SerDeException {
        LOG.debug("Initializing BSONSerde");

        /*
         * column names from the Hive table
         */
        String colNameProp = tblProps.getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(colNameProp.split(","));

        /*
         * Extract Type Information
         */
        String colTypeProp = tblProps.getProperty(Constants.LIST_COLUMN_TYPES);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypeProp);

        assert ( columnNames.size() == columnTypes.size() ) :
                 "Column Names and Types don't match in size";

        numColumns = columnNames.size();

        /*
         * Inspect each column
         */
        List<ObjectInspector> columnInspects = new ArrayList<ObjectInspector>(
                                                    columnNames.size() );
        ObjectInspector inspect;
        for (int c = 0; c < numColumns; c++) {
            inspect = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
                columnTypes.get(c) );
            columnInspects.add( inspect );
        }

        rowInspect = ObjectInspectorFactory.getStandardStructObjectInspector(
                        columnNames, columnInspects);

        /*
         * Create empty row objects which are reused during deser
         */
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }

        stats = new SerDeStats();


        LOG.debug("Completed initialization of BSONSerde.");
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowInspect;
    }

    public Object deserialize(Writable blob) throws SerDeException {
        LOG.info("Deserializing BSON Row with Class: " + blob.getClass());

        BSONObject doc;
        Base64 base64 = new Base64(true);

        if (blob instanceof BSONWritable) {
            BSONWritable b = (BSONWritable) blob;
            LOG.debug("Got a BSONWritable: " + b);
            doc = (BSONObject) b.getDoc();
            if (doc.containsField("_id")) {
                if (doc.get("_id") instanceof ObjectId) {
                	doc.put("id", (base64.encodeToString(((ObjectId) doc.get("_id")).toByteArray())));
                } else
                	doc.put("id", doc.get("_id").toString());
            	doc.removeField("_id");
            }
        } else {
            throw new SerDeException(getClass().toString() +
                    " requires a BSONWritable object, not " + blob.getClass());
        }

        String colName = "";
        Object value = null;
        
        for (int c = 0; c < numColumns; c++) {
            try {
                colName = columnNames.get(c);
                TypeInfo ti = columnTypes.get(c);
                String x = "Col #" + c + " Type: " + ti.getTypeName();
                LOG.trace("***" + x);
                // Attempt typesafe casting
                if (!doc.containsField(colName)) {
                    LOG.debug("Cannot find field '" + colName + "' in " + doc.keySet());
                    for (String k : doc.keySet()) {
                        if (k.trim().equalsIgnoreCase(colName)) {
                            colName = k;
                            LOG.debug("K: " + k + "colName: " + colName);
                        } 
                    }
                }
                if (doc.get(colName) instanceof ObjectId) {
                	value = null;
                } if (ti.getTypeName().equalsIgnoreCase(
                                Constants.DOUBLE_TYPE_NAME)) {
                    value = (Double) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.BIGINT_TYPE_NAME)) {
                    value = (Long) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.INT_TYPE_NAME)) {
                    value = doc.get(colName);
                    /** Some integers end up stored as doubles
                     * due to quirks of the shell
                     */
                    if (value instanceof  Double)
                        value = ((Double) value).intValue();
                    else
                        value = (Integer) value;
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.TINYINT_TYPE_NAME)) {
                    value = (Byte) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.FLOAT_TYPE_NAME)) {
                    value = (Float) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                        Constants.BOOLEAN_TYPE_NAME)) {
                    value = (Boolean) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                                Constants.STRING_TYPE_NAME)) {
            		value = (String) doc.get(colName);
                } else if (ti.getTypeName().equalsIgnoreCase(
                        Constants.DATE_TYPE_NAME) ||
                        ti.getTypeName().equalsIgnoreCase(
                                Constants.DATETIME_TYPE_NAME)  ||
                        ti.getTypeName().equalsIgnoreCase(
                                Constants.TIMESTAMP_TYPE_NAME)) {
                    value = (Date) doc.get(colName);
                } else if (ti.getTypeName().startsWith(Constants.LIST_TYPE_NAME)) {
                    // Copy to an Object array
                    BasicBSONList lst = (BasicBSONList) doc.get(colName);
                    Object[] arr = new Object[lst.size()];
                    for (int i = 0; i < arr.length; i++) {
                            arr[i] = lst.get(i);
                    }
                    value = arr;
                } else if (ti.getTypeName().startsWith(Constants.MAP_TYPE_NAME)) {
                	try {
                		value = ((BSONObject) doc.get(colName)).toMap();
                	} catch(Exception e) {
                		value = null;
                	}
                } else if (ti.getTypeName().startsWith(Constants.STRUCT_TYPE_NAME)) {
                    //value = ((BSONObject) doc).toMap();
                    throw new IllegalArgumentException("Unable to currently work with structs.");
                }  else {
                    // Fall back, just get an object
                    LOG.warn("FALLBACK ON '" + colName + "'");
                    value = doc.get(colName);
                }
                row.set(c, value);
            } catch (Exception e) {
                LOG.error("Exception decoding row at column " + colName, e);
                row.set(c, null);
            }
        }

        LOG.debug("Deserialized Row: " + row);

        return row;
    }

    /**
     * Not sure - something to do with serialization of data
     */
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector oi)
        throws SerDeException {
        return new BSONWritable((BSONObject) serializeStruct(obj, 
                    (StructObjectInspector) oi, ""));
    }
    

    public Object serializeObject(Object obj, ObjectInspector oi, String ext) {
        switch (oi.getCategory()) {
            case LIST:
                return serializeList(obj, (ListObjectInspector) oi, ext);
            case MAP:
                return serializeMap(obj, (MapObjectInspector) oi, ext);
            case PRIMITIVE:
                return serializePrimitive(obj, (PrimitiveObjectInspector) oi);
            case STRUCT:
                return serializeStruct(obj, (StructObjectInspector) oi, ext);
            case UNION:
            default:
                LOG.error("Cannot serialize " + obj.toString() + " of type " + obj.toString());
                break;
        }
        return null;
    }
    

    private Object serializeList(Object obj, ListObjectInspector oi, String ext) {
        BasicBSONList list = new BasicBSONList();
        List<?> field = oi.getList(obj);
        ObjectInspector elemOI = oi.getListElementObjectInspector();
    
        for (Object elem : field) {
            list.add(serializeObject(elem, elemOI, ext));
        }
    
        return list;
    }
    

    /**
     * Turn struct obj into a BasicBSONObject
     */
    private Object serializeStruct(Object obj, 
            StructObjectInspector structOI, 
            String ext) {
        if (ext.length() > 0 && isObjectIdStruct(obj, structOI)) {
            
            String objectIdString = "";
            for (StructField s : structOI.getAllStructFieldRefs()) {
                if (s.getFieldName().equals(this.OID)) {
                    objectIdString = structOI.getStructFieldData(obj, s).toString();
                    break;
                }
            }
            return new ObjectId(objectIdString);
        } else {
        
        	BSONObject bsonObject = new BasicBSONObject();
            // fields is the list of all variable names and information within the struct obj
            List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        
            for (int i = 0 ; i < fields.size() ; i++) {
                StructField field = fields.get(i);
        
                String fieldName, hiveMapping;
                
                // get corresponding mongoDB field  
                if (ext.length() == 0) {
                    fieldName = this.columnNames.get(i);
                    hiveMapping = fieldName;
                } else {
                    fieldName = field.getFieldName();
                    hiveMapping = (ext + "." + fieldName);
                }
                
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = structOI.getStructFieldData(obj, field);
                
                if (this.hiveToMongo != null && this.hiveToMongo.containsKey(hiveMapping)) {
                    String mongoMapping = this.hiveToMongo.get(hiveMapping);
                    int lastDotPos = mongoMapping.lastIndexOf(".");
                    String lastMapping = lastDotPos == -1 ? mongoMapping :  mongoMapping.substring(lastDotPos+1);
                    bsonObject.put(lastMapping,
                                   serializeObject(fieldObj, fieldOI, hiveMapping));
                } else {
                    bsonObject.put(fieldName, 
                                   serializeObject(fieldObj, fieldOI, hiveMapping));   
                }
            }
            
            return bsonObject;
        }
    }    
    
    /**
     *
     * Given a struct, look to se if it contains the fields that a ObjectId
     * struct should contain
     */
    private boolean isObjectIdStruct(Object obj, StructObjectInspector structOI) {
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    
        // If the struct are of incorrect size, then there's no need to create
        // a list of names
        if (fields.size() != 2) {
            return false;
        }
        boolean hasOID = false;
        boolean isBSONType = false;
        for (StructField s : fields) {
            String fieldName = s.getFieldName();
            if (fieldName.equals(this.OID)) {
                hasOID = true;
            } else if (fieldName.equals(this.BSON_TYPE)) {
                String num = structOI.getStructFieldData(obj, s).toString();
                isBSONType = (Integer.parseInt(num) == this.BSON_NUM);
            }

        }
        return hasOID && isBSONType;
    }  
    

    /**
     * For a map of <String, Object> convert to an embedded document 
     */
    private Object serializeMap(Object obj, MapObjectInspector mapOI, String ext) {
        BasicBSONObject bsonObject = new BasicBSONObject();
        ObjectInspector mapValOI = mapOI.getMapValueObjectInspector();
    
        // Each value is guaranteed to be of the same type
        for (Entry<?, ?> entry : mapOI.getMap(obj).entrySet()) {        
            String field = entry.getKey().toString();
            Object value = serializeObject(entry.getValue(), mapValOI, ext);
            bsonObject.put(field, value);
        }
        return bsonObject;
    }

    /**
     * For primitive types, depending on the primitive type, 
     * cast it to types that Mongo supports
     */
    private Object serializePrimitive(Object obj, PrimitiveObjectInspector oi) {
        switch (oi.getPrimitiveCategory()) {
            case TIMESTAMP:
                Timestamp ts = (Timestamp) oi.getPrimitiveJavaObject(obj);
                return new Date(ts.getTime());
            default:
                return oi.getPrimitiveJavaObject(obj);
        }
    }

    public SerDeStats getSerDeStats() {
        stats.setRawDataSize(serializedSize);
        return stats;
    }



    private static final byte[] HEX_CHAR = new byte[] {
            '0' , '1' , '2' , '3' , '4' , '5' , '6' , '7' , '8' ,
            '9' , 'A' , 'B' , 'C' , 'D' , 'E' , 'F'
    };

    protected static void dumpBytes( byte[] buffer ){
        StringBuilder sb = new StringBuilder( 2 + ( 3 * buffer.length ) );

        for ( byte b : buffer ){
            sb.append( "0x" ).append( (char) ( HEX_CHAR[( b & 0x00F0 ) >> 4] ) ).append(
                    (char) ( HEX_CHAR[b & 0x000F] ) ).append( " " );
        }

        LOG.info( "Byte Dump: " + sb.toString() );
    }
}
