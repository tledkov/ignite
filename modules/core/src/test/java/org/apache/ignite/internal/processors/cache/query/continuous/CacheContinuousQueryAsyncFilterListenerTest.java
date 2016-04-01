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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheContinuousQueryAsyncFilterListenerTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    public static final int ITERATION_CNT = 100;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        MemoryEventStorageSpi storeSpi = new MemoryEventStorageSpi();
        storeSpi.setExpireCount(1000);

        cfg.setEventStorageSpi(storeSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    ///
    /// ASYNC FILTER AND LISTENER. TEST LISTENER.
    ///

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerTx() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerTxOffHeap() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, OFFHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerTxOffHeapValues() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, OFFHEAP_VALUES), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomic() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicatedAtomic() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicatedAtomicOffHeapValues() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomicOffHeap() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC, OFFHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomicOffHeapValues() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC, OFFHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerAtomicWithoutBackup() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 0, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListener() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListenerReplicated() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL, ONHEAP_TIERED), true, true);
    }

    ///
    /// ASYNC FILTER AND LISTENER. TEST FILTER.
    ///

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTx() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxOffHeap() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, OFFHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxOffHeapValues() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, OFFHEAP_VALUES), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomic() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedAtomic() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(REPLICATED, 2, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicOffHeap() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, ATOMIC, OFFHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicOffHeapValues() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, ATOMIC, OFFHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicWithoutBackup() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 0, ATOMIC, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilter() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, ONHEAP_TIERED), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicated() throws Exception {
        testNonDeadLockInFilter(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL, ONHEAP_TIERED), true, true);
    }

    ///
    /// ASYNC LISTENER. TEST LISTENER.
    ///

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, ONHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxOffHeapSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, OFFHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterTxOffHeapValuesSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, OFFHEAP_VALUES), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC, ONHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedAtomicSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, ATOMIC, ONHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicOffHeapSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC, OFFHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicOffHeapValuesSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, ATOMIC, OFFHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterAtomicWithoutBackupSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 0, ATOMIC, ONHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, ONHEAP_TIERED), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilterReplicatedSyncFilter() throws Exception {
        testNonDeadLockInListener(cacheConfiguration(REPLICATED, 2, TRANSACTIONAL, ONHEAP_TIERED), false, true);
    }

    /**
     * @param ccfg Cache configuration.
     * @param asyncFilter Async filter.
     * @param asyncListener Async listener.
     * @throws Exception If failed.
     */
    public void testNonDeadLockInListener(CacheConfiguration ccfg,
        final boolean asyncFilter,
        boolean asyncListener) throws Exception {
        final IgniteCache cache = ignite(0).createCache(ccfg);

        try {
            final QueryTestKey key = affinityKey(cache);

            final QueryTestValue val0 = new QueryTestValue(1);
            final QueryTestValue newVal = new QueryTestValue(2);

            for (int i = 0; i < ITERATION_CNT; i++) {
                log.info("Start iteration: " + i);

                ContinuousQuery<QueryTestKey, QueryTestValue> conQry = new ContinuousQuery<>();

                final CountDownLatch latch = new CountDownLatch(1);

                IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> fltrClsr =
                    new IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>>() {
                        @Override public void apply(Ignite ignite, CacheEntryEvent<? extends QueryTestKey,
                            ? extends QueryTestValue> e) {
                            if (asyncFilter) {
                                assertFalse("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("sys-"));

                                assertTrue("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("contQry-"));
                            }
                        }
                    };

                IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> lsnrClsr =
                    new IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>>() {
                    @Override public void apply(Ignite ignite, CacheEntryEvent<? extends QueryTestKey,
                        ? extends QueryTestValue> e) {
                        IgniteCache<Object, Object> cache0 = ignite.cache(cache.getName());

                        QueryTestValue val = e.getValue();

                        if (val == null || !val.equals(new QueryTestValue(1)))
                            return;

                        Transaction tx = null;

                        try {
                            if (cache0.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL)
                                tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                            assertEquals(val, val0);

                            cache0.put(key, newVal);

                            if (tx != null)
                                tx.commit();

                            latch.countDown();
                        }
                        catch (Exception exp) {
                            log.error("Failed: ", exp);

                            throw new IgniteException(exp);
                        }
                        finally {
                            if (tx != null)
                                tx.close();
                        }
                    }
                };

                if (asyncListener)
                    conQry.setLocalListener(new CacheInvokeListenerAsync(lsnrClsr));
                else
                    conQry.setLocalListener(new CacheInvokeListener(lsnrClsr));

                if (asyncFilter)
                    conQry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheTestRemoteFilterAsync(fltrClsr)));
                else
                    conQry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheTestRemoteFilter(fltrClsr)));

                try (QueryCursor qry = cache.query(conQry)) {
                    cache.put(key, val0);

                    assert U.await(latch, 3, SECONDS) : "Failed to waiting event.";

                    assertEquals(cache.get(key), new QueryTestValue(2));
                }

                log.info("Iteration finished: " + i);
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param asyncFilter Async filter.
     * @param asyncListener Async listener.
     * @throws Exception If failed.
     */
    public void testNonDeadLockInFilter(CacheConfiguration ccfg,
        final boolean asyncFilter,
        final boolean asyncListener) throws Exception {
        final IgniteCache cache = ignite(0).createCache(ccfg);

        try {
            final QueryTestKey key = affinityKey(cache);

            final QueryTestValue val0 = new QueryTestValue(1);
            final QueryTestValue newVal = new QueryTestValue(2);

            for (int i = 0; i < ITERATION_CNT; i++) {
                log.info("Start iteration: " + i);

                ContinuousQuery<QueryTestKey, QueryTestValue> conQry = new ContinuousQuery<>();

                final CountDownLatch latch = new CountDownLatch(1);

                IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> fltrClsr =
                    new IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>>() {
                    @Override public void apply(Ignite ignite, CacheEntryEvent<? extends QueryTestKey,
                        ? extends QueryTestValue> e) {
                        if (asyncFilter) {
                            assertFalse("Failed: " + Thread.currentThread().getName(),
                                Thread.currentThread().getName().contains("sys-"));

                            assertTrue("Failed: " + Thread.currentThread().getName(),
                                Thread.currentThread().getName().contains("contQry-"));
                        }



                        IgniteCache<Object, Object> cache0 = ignite.cache(cache.getName());

                        QueryTestValue val = e.getValue();

                        if (val == null || !val.equals(new QueryTestValue(1)))
                            return;

                        Transaction tx = null;

                        try {
                            if (cache0.getConfiguration(CacheConfiguration.class)
                                .getAtomicityMode() == TRANSACTIONAL)
                                tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                            assertEquals(val, val0);

                            cache0.put(key, newVal);

                            if (tx != null)
                                tx.commit();

                            latch.countDown();
                        }
                        catch (Exception exp) {
                            log.error("Failed: ", exp);

                            throw new IgniteException(exp);
                        }
                        finally {
                            if (tx != null)
                                tx.close();
                        }
                    }
                };

                IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> lsnrClsr =
                    new IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>>() {
                        @Override public void apply(Ignite ignite, CacheEntryEvent<? extends QueryTestKey,
                            ? extends QueryTestValue> e) {
                            if (asyncListener) {
                                assertFalse("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("sys-"));

                                assertTrue("Failed: " + Thread.currentThread().getName(),
                                    Thread.currentThread().getName().contains("contQry-"));
                            }

                            QueryTestValue val = e.getValue();

                            if (val == null || !val.equals(new QueryTestValue(1)))
                                return;

                            assertEquals(val, val0);

                            latch.countDown();
                        }
                    };

                if (asyncFilter)
                    conQry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheTestRemoteFilterAsync(fltrClsr)));
                else
                    conQry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheTestRemoteFilter(fltrClsr)));

                if (asyncListener)
                    conQry.setLocalListener(new CacheInvokeListenerAsync(lsnrClsr));
                else
                    conQry.setLocalListener(new CacheInvokeListener(lsnrClsr));

                try (QueryCursor qry = cache.query(conQry)) {
                    cache.put(key, val0);

                    assert U.await(latch, 3, SECONDS) : "Failed to waiting event.";

                    assertEquals(cache.get(key), new QueryTestValue(2));
                }

                log.info("Iteration finished: " + i);
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Ignite cache.
     * @return Key.
     */
    private QueryTestKey affinityKey(IgniteCache cache) {
        Affinity aff = affinity(cache);

        for (int i = 0; i < 10_000; i++) {
            QueryTestKey key = new QueryTestKey(i);

            if (aff.isPrimary(localNode(cache), key))
                return key;
        }

        throw new IgniteException("Failed to found primary key.");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.SECONDS.toMillis(10);
    }

    /**
     *
     */
    @IgniteAsyncCallback
    private static class CacheTestRemoteFilterAsync extends CacheTestRemoteFilter {
        /**
         * @param clsr Closure.
         */
        public CacheTestRemoteFilterAsync(
            IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> clsr) {
            super(clsr);
        }
    }

    /**
     *
     */
    private static class CacheTestRemoteFilter implements
        CacheEntryEventSerializableFilter<QueryTestKey, QueryTestValue> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> clsr;

        /**
         * @param clsr Closure.
         */
        public CacheTestRemoteFilter(IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> clsr) {
            this.clsr = clsr;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e)
            throws CacheEntryListenerException {
            clsr.apply(ignite, e);

            return true;
        }
    }

    /**
     *
     */
    @IgniteAsyncCallback
    private static class CacheInvokeListenerAsync extends CacheInvokeListener {
        /**
         * @param clsr Closure.
         */
        public CacheInvokeListenerAsync(
            IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> clsr) {
            super(clsr);
        }
    }

    /**
     *
     */
    private static class CacheInvokeListener implements CacheEntryUpdatedListener<QueryTestKey, QueryTestValue> {
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> clsr;

        /**
         * @param clsr Closure.
         */
        public CacheInvokeListener(IgniteBiInClosure<Ignite, CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> clsr) {
            this.clsr = clsr;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends QueryTestKey,
            ? extends QueryTestValue>> events)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e : events)
                clsr.apply(ignite, e);
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName("test-cache-" + atomicityMode + "-" + cacheMode + "-" + memoryMode + "-" + backups);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    public static class QueryTestKey implements Serializable, Comparable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public QueryTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestKey that = (QueryTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestKey.class, this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Object o) {
            return key - ((QueryTestKey)o).key;
        }
    }

    /**
     *
     */
    public static class QueryTestValue implements Serializable {
        /** */
        @GridToStringInclude
        protected final Integer val1;

        /** */
        @GridToStringInclude
        protected final String val2;

        /**
         * @param val Value.
         */
        public QueryTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestValue that = (QueryTestValue) o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestValue.class, this);
        }
    }
}
