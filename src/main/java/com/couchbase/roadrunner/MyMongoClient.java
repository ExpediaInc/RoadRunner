package com.couchbase.roadrunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class MyMongoClient implements Client {

    private MongoClient mongoClient;
    private DB db;
    private DBCollection coll; 
    
    public MyMongoClient(GlobalConfig config) throws UnknownHostException {
        List<URI> nodes = config.getNodes();
        List<ServerAddress> servers = new ArrayList<ServerAddress>();
        
        
        for (URI node : nodes) {
            ServerAddress s = new ServerAddress(node.getHost(), node.getPort());
            servers.add(s);
        }
        MongoClient mongoClient = new MongoClient(servers);
        
        db = mongoClient.getDB(config.getBucket());
        coll = db.getCollection(config.getBucket());
        coll.setReadPreference(ReadPreference.nearest());
    }

    @Override
    public Object get(String key) {
        BasicDBObject query = new BasicDBObject("_id", key);
        // The default ReadPreference for the collection is set in the constructor
        DBObject obj = coll.findOne(query);
        return obj;
    }

    @Override
    public boolean set(String key, Object obj) {
        DBObject dbObject;

        if (obj instanceof String) {
            dbObject = (DBObject) JSON.parse((String) obj);
            dbObject.put("_id", key);
        }
        else if (obj instanceof DBObject) {
            dbObject = (DBObject)obj;
        }
        else {
            return false;
        }

        // This will insert if the object doesn't exist, or it will update if it exists.
        // The functionality is needed for the GetSetViewWorkload
        coll.save(dbObject);
        return true;
    }

    @Override
    public Object getCollection(String[] args) {
        List<DBObject> objs = new ArrayList<DBObject>();
        
        DBCursor cursor = coll.find();
        try {
           while(cursor.hasNext()) {
               objs.add(cursor.next());
           }
        } finally {
           cursor.close();
        }
        return objs;

    }

    @Override
    public void shutdown() {
        
        
    }

}
