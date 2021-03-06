/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Main entry point for all <b>Data Grid APIs.</b> You can get a named cache by calling {@link Ignite#cache(String)}
 * method.
 * <h1 class="header">Functionality</h1>
 * This API extends {@link org.gridgain.grid.cache.GridCacheProjection} API which contains vast majority of cache functionality
 * and documentation. In addition to {@link org.gridgain.grid.cache.GridCacheProjection} functionality this API provides:
 * <ul>
 * <li>
 *  Various {@code 'loadCache(..)'} methods to load cache either synchronously or asynchronously.
 *  These methods don't specify any keys to load, and leave it to the underlying storage to load cache
 *  data based on the optionally passed in arguments.
 * </li>
 * <li>
 *     Method {@link #affinity()} provides {@link org.gridgain.grid.cache.affinity.GridCacheAffinityFunction} service for information on
 *     data partitioning and mapping keys to grid nodes responsible for caching those keys.
 * </li>
 * <li>
 *     Method {@link #dataStructures()} provides {@link org.gridgain.grid.cache.datastructures.GridCacheDataStructures} service for
 *     creating and working with distributed concurrent data structures, such as
 *     {@link IgniteAtomicLong}, {@link IgniteAtomicReference}, {@link org.gridgain.grid.cache.datastructures.GridCacheQueue}, etc.
 * </li>
 * <li>
 *  Methods like {@code 'tx{Un}Synchronize(..)'} witch allow to get notifications for transaction state changes.
 *  This feature is very useful when integrating cache transactions with some other in-house transactions.
 * </li>
 * <li>Method {@link #metrics()} to provide metrics for the whole cache.</li>
 * <li>Method {@link #getConfiguration()} to provide cache configuration bean.</li>
 * </ul>
 *
 * @param <K> Cache key type.
 * @param <V> Cache value type.
 */
public interface IgniteCache<K, V> extends javax.cache.Cache<K, V>, IgniteAsyncSupport {
    /** {@inheritDoc} */
    public @Override IgniteCache<K, V> enableAsync();

    /** {@inheritDoc} */
    public @Override <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz);

    /**
     * Gets a random entry out of cache. In the worst cache scenario this method
     * has complexity of <pre>O(S * N/64)</pre> where {@code N} is the size of internal hash
     * table and {@code S} is the number of hash table buckets to sample, which is {@code 5}
     * by default. However, if the table is pretty dense, with density factor of {@code N/64},
     * which is true for near fully populated caches, this method will generally perform significantly
     * faster with complexity of O(S) where {@code S = 5}.
     * <p>
     * Note that this method is not available on {@link org.gridgain.grid.cache.GridCacheProjection} API since it is
     * impossible (or very hard) to deterministically return a number value when pre-filtering
     * and post-filtering is involved (e.g. projection level predicate filters).
     *
     * @return Random entry, or {@code null} if cache is empty.
     */
    @Nullable public Entry<K, V> randomEntry();

    public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc);

    /**
     * Executes {@link #localLoadCache(IgniteBiPredicate, Object...)} on all cache nodes.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link org.gridgain.grid.cache.store.GridCacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @throws CacheException If loading failed.
     */
    public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException;

    /**
     * Delegates to {@link org.gridgain.grid.cache.store.GridCacheStore#loadCache(IgniteBiInClosure,Object...)} method
     * to load state from the underlying persistent storage. The loaded values
     * will then be given to the optionally passed in predicate, and, if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all loaded values will be stored in cache.
     * <p>
     * Note that this method does not receive keys as a parameter, so it is up to
     * {@link org.gridgain.grid.cache.store.GridCacheStore} implementation to provide all the data to be loaded.
     * <p>
     * This method is not transactional and may end up loading a stale value into
     * cache if another thread has updated the value immediately after it has been
     * loaded. It is mostly useful when pre-loading the cache from underlying
     * data store before start, or for read-only caches.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link org.gridgain.grid.cache.store.GridCacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @throws CacheException If loading failed.
     */
    public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException;

    /**
     * Stores given key-value pair in cache only if cache had no previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned.
     * In case of {@link org.gridgain.grid.cache.GridCacheMode#PARTITIONED} or {@link org.gridgain.grid.cache.GridCacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, {@link org.gridgain.grid.cache.store.GridCacheStore#load(IgniteTx, Object)} method will be used.
     * <p>
     * If the returned value is not needed, method {@link #putIfAbsent(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link org.gridgain.grid.cache.store.GridCacheStore}
     * via {@link org.gridgain.grid.cache.store.GridCacheStore#put(IgniteTx, Object, Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.gridgain.grid.cache.GridCacheFlag#LOCAL}, {@link org.gridgain.grid.cache.GridCacheFlag#READ}.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Previously contained value regardless of whether put happened or not.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws CacheException If put operation failed.
     * @throws org.gridgain.grid.cache.GridCacheFlagException If projection flags validation failed.
     */
    @Nullable public V getAndPutIfAbsent(K key, V val) throws CacheException;

    /**
     * Removes mappings from cache for entries for which the optionally passed in filters do
     * pass. If passed in filters are {@code null}, then all entries in cache will be enrolled
     * into transaction.
     * <p>
     * <b>USE WITH CARE</b> - if your cache has many entries that pass through the filter or if filter
     * is empty, then transaction will quickly become very heavy and slow. Also, locks
     * are acquired in undefined order, so it may cause a deadlock when used with
     * other concurrent transactional updates.
     * <p>
     * If write-through is enabled, the values will be removed from {@link org.gridgain.grid.cache.store.GridCacheStore}
     * via {@link org.gridgain.grid.cache.store.GridCacheStore#removeAll(IgniteTx, java.util.Collection)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.gridgain.grid.cache.GridCacheFlag#LOCAL}, {@link org.gridgain.grid.cache.GridCacheFlag#READ}.
     *
     * @param filter Filter used to supply keys for remove operation (if {@code null},
     *      then nothing will be removed).
     * @throws CacheException If remove failed.
     * @throws org.gridgain.grid.cache.GridCacheFlagException If flags validation failed.
     */
    public void removeAll(IgnitePredicate<Entry<K, V>> filter) throws CacheException;

    public Lock lock(K key) throws CacheException;

    public Lock lockAll(Set<? extends K> keys) throws CacheException;

    /**
     * Checks if any node owns a lock for this key.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @param key Key to check.
     * @return {@code True} if lock is owned by some node.
     */
    public boolean isLocked(K key);

    /**
     * Checks if current thread owns a lock on this key.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @param key Key to check.
     * @return {@code True} if key is locked by current thread.
     */
    public boolean isLockedByThread(K key);

    public QueryCursor<Entry<K, V>> query(QueryPredicate<K, V> filter);

    public <R> QueryCursor<R> query(QueryReducer<Entry<K, V>, R> rmtRdc, QueryPredicate<K, V> filter);

    public QueryCursor<List<?>> queryFields(QuerySqlPredicate<K, V> filter);

    public <R> QueryCursor<R> queryFields(QueryReducer<List<?>, R> rmtRdc, QuerySqlPredicate<K, V> filter);

    public QueryCursor<Entry<K, V>> localQuery(QueryPredicate<K, V> filter);

    public QueryCursor<List<?>> localQueryFields(QuerySqlPredicate<K, V> filter);

    public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException;

    public Map<K, V> localPartition(int part) throws CacheException;

    /**
     * Attempts to evict all entries associated with keys. Note,
     * that entry will be evicted only if it's not used (not
     * participating in any locks or transactions).
     * <p>
     * If {@link org.gridgain.grid.cache.GridCacheConfiguration#isSwapEnabled()} is set to {@code true} and
     * {@link org.gridgain.grid.cache.GridCacheFlag#SKIP_SWAP} is not enabled, the evicted entry will
     * be swapped to offheap, and then to disk.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.gridgain.grid.cache.GridCacheFlag#READ}.
     *
     * @param keys Keys to evict.
     */
    public void localEvict(Collection<? extends K> keys);

    /**
     * Peeks at in-memory cached value using default {@link org.gridgain.grid.cache.GridCachePeekMode#SMART}
     * peek mode.
     * <p>
     * This method will not load value from any persistent store or from a remote node.
     * <h2 class="header">Transactions</h2>
     * This method does not participate in any transactions, however, it will
     * peek at transactional value according to the {@link org.gridgain.grid.cache.GridCachePeekMode#SMART} mode
     * semantics. If you need to look at global cached value even from within transaction,
     * you can use {@link org.gridgain.grid.cache.GridCache#peek(Object, java.util.Collection)} method.
     *
     * @param key Entry key.
     * @return Peeked value.
     * @throws NullPointerException If key is {@code null}.
     */
    @Nullable public V localPeek(K key, CachePeekMode... peekModes);

    /**
     * This method unswaps cache entries by given keys, if any, from swap storage
     * into memory.
     * <h2 class="header">Transactions</h2>
     * This method is not transactional.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.gridgain.grid.cache.GridCacheFlag#SKIP_SWAP}, {@link org.gridgain.grid.cache.GridCacheFlag#READ}.
     *
     * @param keys Keys to promote entries for.
     * @throws CacheException If promote failed.
     * @throws org.gridgain.grid.cache.GridCacheFlagException If flags validation failed.
     */
    public void localPromote(Set<? extends K> keys) throws CacheException;

    /**
     * Clears an entry from this cache and swap storage only if the entry
     * is not currently locked, and is not participating in a transaction.
     * <p>
     * If {@link org.gridgain.grid.cache.GridCacheConfiguration#isSwapEnabled()} is set to {@code true} and
     * {@link org.gridgain.grid.cache.GridCacheFlag#SKIP_SWAP} is not enabled, the evicted entries will
     * also be cleared from swap.
     * <p>
     * Note that this operation is local as it merely clears
     * an entry from local cache. It does not remove entries from
     * remote caches or from underlying persistent storage.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link org.gridgain.grid.cache.GridCacheFlag#READ}.
     *
     * @param keys Keys to clear.
     * @return {@code True} if entry was successfully cleared from cache, {@code false}
     *      if entry was in use at the time of this method invocation and could not be
     *      cleared.
     */
    public boolean clear(Collection<? extends K> keys);

    /**
     * Gets the number of all entries cached across all nodes.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size across all nodes.
     */
    public int size(CachePeekMode... peekModes) throws CacheException;

    /**
     * Gets the number of all entries cached on this nodes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size on this node.
     */
    public int localSize(CachePeekMode... peekModes);
}
