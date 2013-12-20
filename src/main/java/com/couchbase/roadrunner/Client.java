package com.couchbase.roadrunner;


public interface Client {

    Object get(String key);
    
    boolean set(String key, Object obj);
    
    Object getCollection(String[] args);
    
    void shutdown();

}
