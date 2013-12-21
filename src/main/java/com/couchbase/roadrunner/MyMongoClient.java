package com.couchbase.roadrunner;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

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
    }

    @Override
    public Object get(String key) {
        BasicDBObject query = new BasicDBObject("id", key);

        DBCursor cursor = coll.find(query);
        DBObject obj = null;
        try {
           if (cursor.hasNext()) {
               obj = cursor.next();
           }
        } finally {
           cursor.close();
        }
        return obj;
    }

    @Override
    public boolean set(String key, Object obj) {
        // TODO Auto-generated method stub
        return false;
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
