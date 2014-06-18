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

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class InternalThreadLocalMap {

    public static final Object UNSET = new Object();

    private static ThreadLocal<InternalThreadLocalMap> slowThreadLocalMap;
    private static final AtomicInteger nextIndex = new AtomicInteger();

    static InternalThreadLocalMap getIfSet() {
        Thread thread = Thread.currentThread();
        InternalThreadLocalMap threadLocalMap;
        if (thread instanceof FastThreadLocalThread) {
            threadLocalMap = ((FastThreadLocalThread) thread).threadLocalMap;
        } else {
            ThreadLocal<InternalThreadLocalMap> slowThreadLocalMap = InternalThreadLocalMap.slowThreadLocalMap;
            if (slowThreadLocalMap == null) {
                threadLocalMap = null;
            } else {
                threadLocalMap = slowThreadLocalMap.get();
            }
        }
        return threadLocalMap;
    }

    public static InternalThreadLocalMap get() {
        Thread thread = Thread.currentThread();
        if (thread instanceof FastThreadLocalThread) {
            return fastGet((FastThreadLocalThread) thread);
        } else {
            return slowGet();
        }
    }

    private static InternalThreadLocalMap fastGet(FastThreadLocalThread thread) {
        InternalThreadLocalMap threadLocalMap = thread.threadLocalMap;
        if (threadLocalMap == null) {
            thread.threadLocalMap = threadLocalMap = new InternalThreadLocalMap();
        }
        return threadLocalMap;
    }

    private static InternalThreadLocalMap slowGet() {
        ThreadLocal<InternalThreadLocalMap> slowThreadLocalMap = InternalThreadLocalMap.slowThreadLocalMap;
        if (slowThreadLocalMap == null) {
            InternalThreadLocalMap.slowThreadLocalMap = slowThreadLocalMap = new ThreadLocal<InternalThreadLocalMap>();
        }

        InternalThreadLocalMap ret = slowThreadLocalMap.get();
        if (ret == null) {
            ret = new InternalThreadLocalMap();
            slowThreadLocalMap.set(ret);
        }
        return ret;
    }

    static void remove() {
        Thread thread = Thread.currentThread();
        if (thread instanceof FastThreadLocalThread) {
            FastThreadLocalThread fastThread = (FastThreadLocalThread) thread;
            fastThread.threadLocalMap = null;
        } else {
            ThreadLocal<InternalThreadLocalMap> slowThreadLocalMap = InternalThreadLocalMap.slowThreadLocalMap;
            if (slowThreadLocalMap != null) {
                slowThreadLocalMap.remove();
            }
        }
    }

    static void destroy() {
        slowThreadLocalMap = null;
    }

    public static int nextVariableIndex() {
        int index = nextIndex.getAndIncrement();
        if (index < 0) {
            nextIndex.decrementAndGet();
            throw new IllegalStateException("too many thread-local indexed variables");
        }
        return index;
    }

    public static int lastVariableIndex() {
        return nextIndex.get() - 1;
    }

    /** Used by {@link FastThreadLocal} */
    private Object[] indexedVariables;

    // Core thread-locals
    private int futureListenerStackDepth;
    private int localChannelReaderStackDepth;
    private Map<Class<?>, Boolean> handlerSharableCache;
    private IntegerHolder counterHashCode;
    private ThreadLocalRandom random;
    private Map<Class<?>, TypeParameterMatcher> typeParameterMatcherGetCache;
    private Map<Class<?>, Map<String, TypeParameterMatcher>> typeParameterMatcherFindCache;

    // String-related thread-locals
    private StringBuilder stringBuilder;
    private Map<Charset, CharsetEncoder> charsetEncoderCache;
    private Map<Charset, CharsetDecoder> charsetDecoderCache;

    // HTTP, SSL, etc
    InternalThreadLocalMap() {
        Object[] array = new Object[32];
        Arrays.fill(array, UNSET);
        indexedVariables = array;
    }

    public StringBuilder stringBuilder() {
        StringBuilder builder = stringBuilder;
        if (builder == null) {
            stringBuilder = builder = new StringBuilder(512);
        } else {
            builder.setLength(0);
        }
        return builder;
    }

    public Map<Charset, CharsetEncoder> charsetEncoderCache() {
        Map<Charset, CharsetEncoder> cache = charsetEncoderCache;
        if (cache == null) {
            charsetEncoderCache = cache = new IdentityHashMap<Charset, CharsetEncoder>();
        }
        return cache;
    }

    public Map<Charset, CharsetDecoder> charsetDecoderCache() {
        Map<Charset, CharsetDecoder> cache = charsetDecoderCache;
        if (cache == null) {
            charsetDecoderCache = cache = new IdentityHashMap<Charset, CharsetDecoder>();
        }
        return cache;
    }

    public int futureListenerStackDepth() {
        return futureListenerStackDepth;
    }

    public void setFutureListenerStackDepth(int futureListenerStackDepth) {
        this.futureListenerStackDepth = futureListenerStackDepth;
    }

    public ThreadLocalRandom random() {
        ThreadLocalRandom r = random;
        if (r == null) {
            random = r = new ThreadLocalRandom();
        }
        return r;
    }

    public Map<Class<?>, TypeParameterMatcher> typeParameterMatcherGetCache() {
        Map<Class<?>, TypeParameterMatcher> cache = typeParameterMatcherGetCache;
        if (cache == null) {
            typeParameterMatcherGetCache = cache = new IdentityHashMap<Class<?>, TypeParameterMatcher>();
        }
        return cache;
    }

    public Map<Class<?>, Map<String, TypeParameterMatcher>> typeParameterMatcherFindCache() {
        Map<Class<?>, Map<String, TypeParameterMatcher>> cache = typeParameterMatcherFindCache;
        if (cache == null) {
            typeParameterMatcherFindCache = cache = new IdentityHashMap<Class<?>, Map<String, TypeParameterMatcher>>();
        }
        return cache;
    }

    public IntegerHolder counterHashCode() {
        return counterHashCode;
    }

    public void setCounterHashCode(IntegerHolder counterHashCode) {
        this.counterHashCode = counterHashCode;
    }

    public Map<Class<?>, Boolean> handlerSharableCache() {
        Map<Class<?>, Boolean> cache = handlerSharableCache;
        if (cache == null) {
            // Start with small capacity to keep memory overhead as low as possible.
            handlerSharableCache = cache = new WeakHashMap<Class<?>, Boolean>(4);
        }
        return cache;
    }

    public int localChannelReaderStackDepth() {
        return localChannelReaderStackDepth;
    }

    public void setLocalChannelReaderStackDepth(int localChannelReaderStackDepth) {
        this.localChannelReaderStackDepth = localChannelReaderStackDepth;
    }

    public Object indexedVariable(int index) {
        Object[] lookup = indexedVariables;
        if (index >= lookup.length) {
            expandArray(index);
            return UNSET;
        }

        return lookup[index];
    }

    public void setIndexedVariable(int index, Object value) {
        Object[] lookup = indexedVariables;
        if (index >= lookup.length) {
            lookup = expandArray(index);
        }
        lookup[index] = value;
    }

    public void removeIndexedVariable(int index) {
        Object[] lookup = indexedVariables;
        if (index < lookup.length) {
            lookup[index] = UNSET;
        }
    }

    public boolean isIndexedVariableSet(int index) {
        Object[] lookup = indexedVariables;
        return index < lookup.length && lookup[index] != UNSET;
    }

    private Object[] expandArray(int length) {
        final int oldCapacity = indexedVariables.length;
        int newCapacity = oldCapacity;
        do {
            // double capacity until it is big enough
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (length > newCapacity);

        Object[] array = Arrays.copyOf(indexedVariables, newCapacity);
        Arrays.fill(array, oldCapacity, array.length, UNSET);
        return indexedVariables = array;
    }
}
