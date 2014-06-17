/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util.internal;

import io.netty.util.ThreadDeathWatcher;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class FastThreadLocalTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Remove the cruft from the previous tests.
        FastThreadLocal.removeAll();
        assertThat(ThreadDeathWatcher.awaitInactivity(10, TimeUnit.SECONDS), is(true));
    }

    @Test(timeout = 10000)
    public void testRemoveAll() throws Exception {
        final AtomicBoolean removed = new AtomicBoolean();
        final ThreadLocal<Boolean> var = new FastThreadLocal<Boolean>() {
            @Override
            public void remove() {
                super.remove();
                removed.set(true);
            }
        };

        // Initialize a thread-local variable.
        assertThat(var.get(), is(nullValue()));

        // And then remove it.
        FastThreadLocal.removeAll();

        assertThat(removed.get(), is(true));

        ThreadDeathWatcher.awaitInactivity(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    @Test(timeout = 10000)
    public void testRemoveAllFromFTLThread() throws Throwable {
        final AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
        final Thread thread = new FastThreadLocalThread() {
            @Override
            public void run() {
                try {
                    testRemoveAll();
                } catch (Throwable t) {
                    throwable.set(t);
                }
            }
        };

        thread.start();
        thread.join();

        Throwable t = throwable.get();
        if (t != null) {
            throw t;
        }

        ThreadDeathWatcher.awaitInactivity(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
