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

/**
 * Returns workloads based on their string representation.
 */
public class WorkloadFactory {

    /**
     * Returns a workload based on its identifier.
     * 
     * @param identifier
     * @return the workload
     */
    public static Class<? extends Workload> getWorkload(String identifier) {
        if (identifier.equals("getset")) {
            return GetSetWorkload.class;
        } else if (identifier.equals("getscas")) {
            return GetsCasWorkload.class;
        } else if (identifier.equals("getsetview")) {
            return GetSetViewWorkload.class;
        }

        throw new IllegalArgumentException("Could not find Workload: " + identifier);
    }
}
