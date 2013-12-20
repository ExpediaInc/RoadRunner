package com.couchbase.roadrunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;

public class MyCouchbaseClient implements Client {
    private CouchbaseClient couchbaseClient;

    public MyCouchbaseClient(GlobalConfig config) throws IOException {
        couchbaseClient = new CouchbaseClient(config.getNodes(), config.getBucket(), config.getPassword());
    }

    @Override
    public Object get(String key) {
        return couchbaseClient.get(key);
    }

    @Override
    public boolean set(String key, Object obj) {
        try {
            return couchbaseClient.set(key, obj).get();
        } catch (InterruptedException e) {
            return false;
        } catch (ExecutionException e) {
            return false;
        }
    }

    @Override
    public Object getCollection(String[] args) {
        View view = couchbaseClient.getView(args[0], args[1]);
        Query query = new Query();
        query.setIncludeDocs(true);
        query.setStale(Stale.OK);
        return couchbaseClient.query(view, query);
    }

    @Override
    public void shutdown() {
        couchbaseClient.shutdown();
    }
    
}
