/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.roadrunner.workloads;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.google.common.base.Stopwatch;

public class GetSetViewWorkload extends Workload {

    /** Amount of documents to set/get. */
    private final long amount;

    /** Ratio between get and set calls. */
    private final String ratio;

    /** Ratio between set:get:view calls. */
    private final int writeRatio;
    private final int readRatio;
    private final int viewRatio;

    /** Ratio to sample statistics data. */
    private final int sampling;
    
    private static final String DESIGN_DOC = "auth_design";
    private static final String VIEW = "auth_view";

    public GetSetViewWorkload(CouchbaseClient client, String name, long amount, String ratio, int sampling, int ramp,
            DocumentFactory documentFactory) {
        super(client, name, ramp, documentFactory);
        this.amount = amount;
        
        String[] arr = ratio.split(":");
        if (arr.length != 3) {
            throw new IllegalArgumentException("getsetview workload requires ratio of 3 numbers (write:read:view)");
        }
        List<Integer> ratios = new ArrayList<Integer>();

        for (int n = 0; n < arr.length; n++) {
            ratios.add(Integer.parseInt(arr[n]));
        }

        int min = Collections.min(ratios);
        if (min > 0) {
            writeRatio = Math.round(ratios.get(0) / min);
            readRatio = Math.round(ratios.get(1) / min);
            viewRatio = Math.round(ratios.get(2) / min);
        } else {
            writeRatio = Math.round(ratios.get(0));
            readRatio = Math.round(ratios.get(1));
            viewRatio = Math.round(ratios.get(2));
        }
        this.ratio = ratio;
        this.sampling = 100 / sampling;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(getWorkloadName());
        startTimer();

        int samplingCount = 0;
        for (long i = 0; i < amount; i++) {
            String key = randomKey();
            try {
                if (++samplingCount == sampling) {
                    for (int w = 0; w < writeRatio; w++) {
                        setWorkloadWithMeasurement(key);
                    }

                    for (int r = 0; r < readRatio; r++) {
                        getWorkloadWithMeasurement(key);
                    }

                    for (int v = 0; v < viewRatio; v++) {
                        viewWorkloadWithMeasurement();
                    }
                    samplingCount = 0;
                } else {

                    for (int w = 0; w < writeRatio; w++) {
                        setWorkload(key);
                    }

                    for (int r = 0; r < readRatio; r++) {
                        getWorkload(key);
                    }

                    for (int v = 0; v < viewRatio; v++) {
                        viewWorkload();
                    }

                }
            } catch (Exception ex) {
                getLogger().info("Problem while set/get/view key" + ex.getMessage());
            }
        }

        endTimer();
    }

    private void setWorkloadWithMeasurement(String key) throws Exception {
        Stopwatch watch = new Stopwatch().start();
        setWorkload(key);
        watch.stop();
        addMeasure("set", watch);
    }

    private void setWorkload(String key) throws Exception {
        getClient().set(key, 0, getDocument()).get();
        incrTotalOps();
    }

    private void getWorkloadWithMeasurement(String key) throws Exception {
        Stopwatch watch = new Stopwatch().start();
        getWorkload(key);
        watch.stop();
        addMeasure("get", watch);
    }

    private void getWorkload(String key) throws Exception {
        getClient().get(key);
        incrTotalOps();
    }

    private void viewWorkloadWithMeasurement() throws Exception {
        Stopwatch watch = new Stopwatch().start();
        viewWorkload();
        watch.stop();
        addMeasure("view", watch);
    }

    private void viewWorkload() throws Exception {
        View view = getClient().getView(DESIGN_DOC, VIEW);
        Query query = new Query();
        query.setStale(Stale.OK);
        getClient().query(view, query);
        incrTotalOps();
    }
}
