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
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A special {@link ThreadLocal} which is operating over a predefined array, so it always operate in O(1) when called
 * from a {@link FastThreadLocalThread}. This permits less indirection and offers a slight performance improvement,
 * so is useful when invoked frequently.
 *
 * The fast path is only possible on threads that extend FastThreadLocalThread, as this class
 * stores the necessary state. Access by any other kind of thread falls back to a regular ThreadLocal
 *
 * @param <V>
 */
public class FastThreadLocal<V> extends ThreadLocal<V> {

    static final Object EMPTY = new Object();

    private static final AtomicInteger NEXT_INDEX = new AtomicInteger(0);

    /**
     * Keeps the live threads and their thread-local variables.
     *
     * A new entry is added by {@link #add(Thread, FastThreadLocal)}.
     * An entry is removed by:
     *
     * - {@link RemoveTask#run()} invoked by {@link ThreadDeathWatcher} when the thread dies, or
     * - {@link #removeAll()} invoked by a user or {@link DefaultThreadFactory}'s default decorator task.
     *
     * Note that we do not use {@link ConcurrentHashMapV8} or {@link PlatformDependent#newConcurrentHashMap()}
     * because we use our own thread-local variables there, too.
     */
    private static final Map<Thread, RemoveTask> allThreads = new ConcurrentHashMap<Thread, RemoveTask>();

    /**
     * Removes all thread local variables initialized by {@link FastThreadLocal} in the current thread.
     */
    public static void removeAll() {
        removeAll(Thread.currentThread());
    }

    private static void removeAll(Thread thread) {
        RemoveTask task = allThreads.remove(thread);
        if (task != null) {
            ThreadDeathWatcher.unwatch(thread, task);
            for (FastThreadLocal<?> v: task.threadLocals) {
                v.remove();
            }
        }
    }

    /**
     * Adds the specified thread-local variable to the list of all initialized thread-local variables.
     */
    static void add(Thread currentThread, FastThreadLocal<?> variable) {
        assert currentThread == Thread.currentThread();
        RemoveTask task = allThreads.get(currentThread);
        if (task == null) {
            task = new RemoveTask(currentThread);
            allThreads.put(currentThread, task);
            ThreadDeathWatcher.watch(currentThread, task);
        }

        task.threadLocals.add(variable);
    }

    private static final class RemoveTask implements Runnable {

        private final Thread thread;

        /**
         * Why use Set instead of List or Queue?
         * To avoid duplicate insertion when a thread-local variable is removed and then initialized again.
         */
        private final Set<FastThreadLocal<?>> threadLocals =
                Collections.newSetFromMap(new IdentityHashMap<FastThreadLocal<?>, Boolean>());

        RemoveTask(Thread thread) {
            this.thread = thread;
        }

        /**
         * Invoked by {@link ThreadDeathWatcher} when a user did not call {@link #removeAll()} before the thread dies.
         */
        @Override
        public void run() {
            allThreads.remove(thread);
        }
    }

    private final ThreadLocal<V> fallback = new ThreadLocal<V>() {
        @Override
        protected V initialValue() {
            add(Thread.currentThread(), FastThreadLocal.this);
            return FastThreadLocal.this.initialValue();
        }
    };
    private final int index;

    public FastThreadLocal() {
        index = NEXT_INDEX.getAndIncrement();
        if (index < 0) {
            NEXT_INDEX.decrementAndGet();
            throw new IllegalStateException("Maximal number (" + Integer.MAX_VALUE + ") of FastThreadLocal exceeded");
        }
    }

    /**
     * Set the value for the current thread
     */
    @Override
    public void set(V value) {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof FastThreadLocalThread)) {
            fallback.set(value);
            return;
        }
        FastThreadLocalThread fastThread = (FastThreadLocalThread) thread;
        Object[] lookup = fastThread.lookup;
        if (index >= lookup.length) {
            lookup = fastThread.expandArray(index);
        }
        lookup[index] = value;
    }

    /**
     * Sets the value to uninitialized; a proceeding call to get() will trigger a call to initialValue()
     */
    @Override
    public void remove() {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof FastThreadLocalThread)) {
            fallback.remove();
            return;
        }

        Object[] lookup = ((FastThreadLocalThread) thread).lookup;
        if (index >= lookup.length) {
            return;
        }
        lookup[index] = EMPTY;
    }

    /**
     * @return the current value for the current thread
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get() {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof FastThreadLocalThread)) {
            return fallback.get();
        }
        FastThreadLocalThread fastThread = (FastThreadLocalThread) thread;

        Object[] lookup = fastThread.lookup;
        Object v;
        if (index >= lookup.length) {
            add(thread, this);
            v = initialValue();
            lookup = fastThread.expandArray(index);
            lookup[index] = v;
        } else {
            v = lookup[index];
            if (v == EMPTY) {
                add(thread, this);
                v = initialValue();
                lookup[index] = v;
            }
        }
        return (V) v;
    }
}
