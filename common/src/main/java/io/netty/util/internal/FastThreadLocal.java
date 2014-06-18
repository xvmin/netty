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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

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
public class FastThreadLocal<V> {

    private static final int variablesToRemoveIndex = InternalThreadLocalMap.nextVariableIndex();

    public static void removeAll() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return;
        }

        try {
            Object v = threadLocalMap.indexedVariable(variablesToRemoveIndex);
            if (v != null && v != InternalThreadLocalMap.UNSET) {
                @SuppressWarnings("unchecked")
                Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
                for (FastThreadLocal<?> tlv: variablesToRemove) {
                    tlv.remove(threadLocalMap);
                }
            }
        } finally {
            InternalThreadLocalMap.remove();
        }
    }

    public static void destroy() {
        InternalThreadLocalMap.destroy();
    }

    @SuppressWarnings("unchecked")
    private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
        Object v = threadLocalMap.indexedVariable(variablesToRemoveIndex);
        Set<FastThreadLocal<?>> variablesToRemove;
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<FastThreadLocal<?>, Boolean>());
            threadLocalMap.setIndexedVariable(variablesToRemoveIndex, variablesToRemove);
        } else {
            variablesToRemove = (Set<FastThreadLocal<?>>) v;
        }

        variablesToRemove.add(variable);
    }

    private static void removeFromVariablesToRemove(
            InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {

        Object v = threadLocalMap.indexedVariable(variablesToRemoveIndex);

        if (v == InternalThreadLocalMap.UNSET || v == null) {
            return;
        }

        @SuppressWarnings("unchecked")
        Set<FastThreadLocal<?>> variablesToRemove = (Set<FastThreadLocal<?>>) v;
        variablesToRemove.remove(variable);
    }

    private final int index;

    public FastThreadLocal() {
        index = InternalThreadLocalMap.nextVariableIndex();
    }

    /**
     * Returns the current value for the current thread
     */
    public final V get() {
        return get(InternalThreadLocalMap.get());
    }

    /**
     * Returns the current value for the specified thread local map.
     * The specified thread local map must be for the current thread.
     */
    @SuppressWarnings("unchecked")
    public V get(InternalThreadLocalMap threadLocalMap) {
        Object v = threadLocalMap.indexedVariable(index);
        if (v == InternalThreadLocalMap.UNSET) {
            v = initialValue();
            threadLocalMap.setIndexedVariable(index, v);
            addToVariablesToRemove(threadLocalMap, this);
        }
        return (V) v;
    }

    /**
     * Set the value for the current thread.
     */
    public final void set(V value) {
        if (value == InternalThreadLocalMap.UNSET) {
            remove();
        } else {
            set(InternalThreadLocalMap.get(), value);
        }
    }

    /**
     * Set the value for the specified thread local map. The specified thread local map must be for the current thread.
     */
    public void set(InternalThreadLocalMap threadLocalMap, V value) {
        if (value == InternalThreadLocalMap.UNSET) {
            remove(threadLocalMap);
        } else {
            threadLocalMap.setIndexedVariable(index, value);
        }
    }

    /**
     * Returns {@code true} if and only if this thread-local variable is set.
     */
    public final boolean isSet() {
        return isSet(InternalThreadLocalMap.getIfSet());
    }

    /**
     * Returns {@code true} if and only if this thread-local variable is set.
     * The specified thread local map must be for the current thread.
     */
    public boolean isSet(InternalThreadLocalMap threadLocalMap) {
        return threadLocalMap != null && threadLocalMap.isIndexedVariableSet(index);
    }
    /**
     * Sets the value to uninitialized; a proceeding call to get() will trigger a call to initialValue().
     */
    public final void remove() {
        remove(InternalThreadLocalMap.getIfSet());
    }

    /**
     * Sets the value to uninitialized for the specified thread local map;
     * a proceeding call to get() will trigger a call to initialValue().
     * The specified thread local map must be for the current thread.
     */
    public void remove(InternalThreadLocalMap threadLocalMap) {
        if (threadLocalMap == null) {
            return;
        }

        threadLocalMap.removeIndexedVariable(index);
        removeFromVariablesToRemove(threadLocalMap, this);
    }

    /**
     * Returns the initial value for this thread-local variable.
     */
    protected V initialValue() {
        return null;
    }
}
