package org.apache.kafka.connect.mongodb;

import static com.mongodb.client.model.Filters.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.bson.Document;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import junit.framework.TestCase;

/**
 * @author Andrea Patelli
 */
public class MongodbSinkTaskTest extends TestCase {

    private static String REPLICATION_PATH = "/tmp/mongo";
    private MongodbSinkTask task;
    private SinkTaskContext context;
    private Map<String, String> sinkProperties;

    private MongodExecutable mongodExecutable;
    private MongodProcess mongod;
    private MongodStarter mongodStarter;
    private IMongodConfig mongodConfig;
    private MongoClient mongoClient;


    @Override
    public void setUp() {
        try {
            super.setUp();
            mongodStarter = MongodStarter.getDefaultInstance();
            mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.V3_2)
                    .replication(new Storage(REPLICATION_PATH, "rs0", 1024))
                    .net(new Net(12345, Network.localhostIsIPv6()))
                    .build();
            mongodExecutable = mongodStarter.prepare(mongodConfig);
            mongod = mongodExecutable.start();
            mongoClient = new MongoClient(new ServerAddress("localhost", 12345));
            MongoDatabase adminDatabase = mongoClient.getDatabase("admin");

            BasicDBObject replicaSetSetting = new BasicDBObject();
            replicaSetSetting.put("_id", "rs0");
            BasicDBList members = new BasicDBList();
            DBObject host = new BasicDBObject();
            host.put("_id", 0);
            host.put("host", "127.0.0.1:12345");
            members.add(host);
            replicaSetSetting.put("members", members);
            adminDatabase.runCommand(new BasicDBObject("isMaster", 1));
            adminDatabase.runCommand(new BasicDBObject("replSetInitiate", replicaSetSetting));
            MongoDatabase db = mongoClient.getDatabase("mydb");
        } catch (Exception e) {
//                Assert.assertTrue(false);
        }

        task = new MongodbSinkTask();

        context = PowerMock.createMock(SinkTaskContext.class);
        task.initialize(context);

        sinkProperties = new HashMap<>();
        sinkProperties.put("host", "localhost");
        sinkProperties.put("port", Integer.toString(12345));
        sinkProperties.put("bulk.size", Integer.toString(100));
        sinkProperties.put("topics", "test-topic");
        sinkProperties.put("mongodb.database", "mydb");
        sinkProperties.put("mongodb.collections", "test-collection");

        task.start(sinkProperties);
    }
    @Test
    public void testUpsertRecordsWithOffset(){
    	Collection<SinkRecord> records = new ArrayList<SinkRecord>();
    	records.add(buildSimpleValueRecord("1", 1111));
    	records.add(buildSimpleValueRecord("2", 1112));
    	records.add(buildSimpleValueRecord("3", 1113));
    	
		task.put(records);
		
        MongoDatabase db = mongoClient.getDatabase("mydb");
        MongoCollection<Document> documents = db.getCollection("test-collection");
        assertEquals(3,documents.count());
        
        //Check the first one
		Document myDoc = documents.find(eq("_id",1111)).first();
		assertNotNull(myDoc);
		assertEquals("1",myDoc.getString("value"));
		
    }
	
    
    @Test
    public void testUpsertRecordsWithKey(){
    	Collection<SinkRecord> records = new ArrayList<SinkRecord>();
    	records.add(buildSimpleKeyValueRecord("1", "10", 1111));
    	records.add(buildSimpleKeyValueRecord("2", "20", 1112));
    	records.add(buildSimpleKeyValueRecord("3", "30", 1113));
    	records.add(buildSimpleKeyValueRecord("2", "22", 1114));
    	
		task.put(records);
		
        MongoDatabase db = mongoClient.getDatabase("mydb");
        MongoCollection<Document> documents = db.getCollection("test-collection");
        
        assertEquals(3,documents.count());
        
        int indexCount = 0;
        MongoCursor<Document> indices = documents.listIndexes().iterator();
		while (indices.hasNext()) { indices.next(); indexCount++; }
        assertEquals("Should create a second unique index.",2,indexCount);
        
        
        MongoCursor<Document> cursor = documents.find().iterator();
        while(cursor.hasNext()){
        	Document doc = cursor.next();
        	System.out.println(doc.toJson());
    	}
        //Check 1 
		Document myDoc = documents.find(eq("key","1")).first();
		assertNotNull(myDoc);
		assertEquals("10",myDoc.getString("value"));
        
		//Check 2
		myDoc = documents.find(eq("key","2")).first();
		assertNotNull(myDoc);
		assertEquals("22",myDoc.getString("value"));
    }
    @Test
    public void testUpsertRecordsWithComplexKey(){
    	Collection<SinkRecord> records = new ArrayList<SinkRecord>();
    	records.add(buildComplexKeyValueRecord("1","9", "10", 1111));
    	records.add(buildComplexKeyValueRecord("2","8", "20", 1112));
    	records.add(buildComplexKeyValueRecord("3","7", "30", 1113));
    	records.add(buildComplexKeyValueRecord("2","8", "22", 1114));
    	
		task.put(records);
		
        MongoDatabase db = mongoClient.getDatabase("mydb");
        MongoCollection<Document> documents = db.getCollection("test-collection");
        
        assertEquals(3,documents.count());
        
        int indexCount = 0;
        MongoCursor<Document> indices = documents.listIndexes().iterator();
		while (indices.hasNext()) { indices.next(); indexCount++; }
        assertEquals("Should create a second unique index.",2,indexCount);
        
        
        MongoCursor<Document> cursor = documents.find().iterator();
        while(cursor.hasNext()){
        	Document doc = cursor.next();
        	System.out.println(doc.toJson());
    	}
        //Check 1 
		Document myDoc = documents.find(and(eq("keyPart1","1"),eq("keyPart2","9"))).first();
		assertNotNull(myDoc);
		assertEquals("10",myDoc.getString("value"));
        
		//Check 2
		myDoc = documents.find(and(eq("keyPart1","2"),eq("keyPart2","8"))).first();
		assertNotNull(myDoc);
		assertEquals("22",myDoc.getString("value"));
    }
    
    private SinkRecord buildSimpleValueRecord(String simpleStringValue, long kafkaOffset) {
		String topic = "test-topic";
		int partition = 0;
		Schema keySchema = null;
		Object key = null;
		Schema valueSchema = SchemaBuilder.struct().field("value",SchemaBuilder.string().build()).build();
		Object value = new Struct(valueSchema).put("value", simpleStringValue);
		SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, kafkaOffset);
		return sinkRecord;
	}
	private SinkRecord buildSimpleKeyValueRecord(String simpleStringKey, String simpleStringValue, long kafkaOffset) {
		String topic = "test-topic";
		int partition = 0;
		Schema keySchema = SchemaBuilder.struct().field("key",SchemaBuilder.string().build()).build();;
		Object key = new Struct(keySchema).put("key", simpleStringKey);
		//Put key in value as well
		Schema valueSchema = SchemaBuilder.struct()
				.field("value",SchemaBuilder.string().build())
				.field("key",SchemaBuilder.string().build())
				.build();
		Object value = new Struct(valueSchema)
				.put("value", simpleStringValue)
				.put("key", simpleStringValue);
		SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, kafkaOffset);
		return sinkRecord;
	}
	private SinkRecord buildComplexKeyValueRecord(String stringKeyPart1,String stringKeyPart2, String simpleStringValue, long kafkaOffset) {
		String topic = "test-topic";
		int partition = 0;
		Schema keySchema = SchemaBuilder.struct()
				.field("keyPart1",SchemaBuilder.string().build())
				.field("keyPart2",SchemaBuilder.string().build()).build();
		Object key = new Struct(keySchema)
				.put("keyPart1", stringKeyPart1)
				.put("keyPart2", stringKeyPart2);
		//Put key in value as well
		Schema valueSchema = SchemaBuilder.struct()
				.field("value",SchemaBuilder.string().build())
				.field("key",SchemaBuilder.string().build())
				.build();
		Object value = new Struct(valueSchema)
				.put("value", simpleStringValue)
				.put("key", simpleStringValue);
		SinkRecord sinkRecord = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, kafkaOffset);
		return sinkRecord;
	}

    @Override
    public void tearDown() {
        try {
            super.tearDown();
            mongod.stop();
            mongodExecutable.stop();
            System.out.println("DELETING OPLOG");
            FileUtils.deleteDirectory(new File(REPLICATION_PATH));
        } catch (Exception e) {
        }
    }
}
