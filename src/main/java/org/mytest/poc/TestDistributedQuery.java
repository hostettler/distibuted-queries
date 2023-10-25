package org.mytest.poc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.mytest.poc.model.EntityA;
import org.mytest.poc.model.EntityAKey;
import org.mytest.poc.model.EntityB;
import org.mytest.poc.model.EntityBKey;
import org.mytest.poc.model.EntityC;
import org.mytest.poc.model.EntityCKey;
import org.mytest.poc.model.EntityD;
import org.mytest.poc.model.EntityDKey;

public class TestDistributedQuery {

    private static final String A_CACHE = EntityA.class.getSimpleName();
    private static final String B_CACHE = EntityB.class.getSimpleName();
    private static final String C_CACHE = EntityC.class.getSimpleName();
    private static final String D_CACHE = EntityD.class.getSimpleName();
    private static final int NB_ENTITY_A = 5000;
    private static final int NB_ENTITY_B = 4;
    private static final int NB_ENTITY_C = 5000;
    private static final int NB_ENTITY_D = 5;
    private static final int PARRALEL_LEVEL = 12;

    public static void main(String[] args) throws IgniteException, FileNotFoundException {
        try (Ignite ignite = Ignition.start(new FileInputStream("config/ignite-config.xml"))) {
            System.out.println();
            System.out.println(">>> SQL queries example started.");

            // ----------------------------------------------------------------------------------------
            CacheConfiguration<EntityAKey, EntityA> entityACacheCfg = new CacheConfiguration<>(A_CACHE);
            entityACacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            QueryEntity queryEntity = new QueryEntity();
            queryEntity.addQueryField("id", String.class.getName(), null);
            queryEntity.addQueryField("value", String.class.getName(), null);
            queryEntity.addQueryField("entityC", String.class.getName(), null);
            queryEntity.setTableName(EntityA.class.getSimpleName());
            queryEntity.setKeyType(EntityAKey.class.getCanonicalName());
            queryEntity.setValueType(EntityA.class.getCanonicalName());
            Set<String> keyFields = Set.of("id");
            queryEntity.setKeyFields(keyFields);
            Collection<QueryIndex> indexedColumns = new ArrayList<>();
            indexedColumns.add(new QueryIndex("id"));
            queryEntity.setIndexes(indexedColumns);
            entityACacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityAKey.class).setAffinityKeyFieldName("id"));
            entityACacheCfg.setQueryEntities(Set.of(queryEntity));
            entityACacheCfg.setIndexedTypes(EntityAKey.class, EntityA.class);
            entityACacheCfg.setQueryParallelism(PARRALEL_LEVEL);

            // ----------------------------------------------------------------------------------------
            CacheConfiguration<EntityBKey, EntityB> entityBCacheCfg = new CacheConfiguration<>(B_CACHE);
            entityBCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            queryEntity = new QueryEntity();
            queryEntity.addQueryField("id", String.class.getName(), null);
            queryEntity.addQueryField("entityA", String.class.getName(), null);
            queryEntity.addQueryField("value", String.class.getName(), null);
            queryEntity.setTableName(EntityB.class.getSimpleName());
            queryEntity.setKeyType(EntityBKey.class.getCanonicalName());
            queryEntity.setValueType(EntityB.class.getCanonicalName());
            keyFields = Set.of("id", "entityA");
            queryEntity.setKeyFields(keyFields);
            indexedColumns = new ArrayList<>();
            indexedColumns.add(new QueryIndex("id"));
            indexedColumns.add(new QueryIndex("entityA"));
            queryEntity.setIndexes(indexedColumns);
            entityBCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityBKey.class).setAffinityKeyFieldName("entityA"));
            entityBCacheCfg.setQueryEntities(Set.of(queryEntity));
            entityBCacheCfg.setIndexedTypes(EntityBKey.class, EntityB.class);
            entityBCacheCfg.setQueryParallelism(PARRALEL_LEVEL);

            // ----------------------------------------------------------------------------------------
            CacheConfiguration<EntityCKey, EntityC> entityCCacheCfg = new CacheConfiguration<>(C_CACHE);
            entityCCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            queryEntity = new QueryEntity();
            queryEntity.addQueryField("id", String.class.getName(), null);
            queryEntity.addQueryField("value", String.class.getName(), null);
            queryEntity.setTableName(EntityC.class.getSimpleName());
            queryEntity.setKeyType(EntityCKey.class.getCanonicalName());
            queryEntity.setValueType(EntityC.class.getCanonicalName());
            keyFields = Set.of("id");
            queryEntity.setKeyFields(keyFields);
            indexedColumns = Collections.singleton(new QueryIndex("id"));
            queryEntity.setIndexes(indexedColumns);
            entityCCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityCKey.class).setAffinityKeyFieldName("id"));
            entityCCacheCfg.setQueryEntities(Set.of(queryEntity));
            entityCCacheCfg.setIndexedTypes(EntityCKey.class, EntityC.class);
            entityCCacheCfg.setQueryParallelism(PARRALEL_LEVEL);

            // ----------------------------------------------------------------------------------------
            CacheConfiguration<EntityDKey, EntityD> entityDCacheCfg = new CacheConfiguration<>(D_CACHE);
            entityDCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            queryEntity = new QueryEntity();
            queryEntity.addQueryField("id", String.class.getName(), null);
            queryEntity.addQueryField("entityC", String.class.getName(), null);
            queryEntity.addQueryField("value", String.class.getName(), null);
            queryEntity.setTableName(EntityD.class.getSimpleName());
            queryEntity.setKeyType(EntityDKey.class.getCanonicalName());
            queryEntity.setValueType(EntityD.class.getCanonicalName());
            keyFields = Set.of("id", "entityC");
            queryEntity.setKeyFields(keyFields);
            indexedColumns = new ArrayList<>();
            indexedColumns.add(new QueryIndex("id"));
            indexedColumns.add(new QueryIndex("entityC"));
            queryEntity.setIndexes(indexedColumns);
            entityDCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityDKey.class).setAffinityKeyFieldName("entityC"));
            entityDCacheCfg.setQueryEntities(Set.of(queryEntity));
            entityDCacheCfg.setIndexedTypes(EntityDKey.class, EntityD.class);
            entityDCacheCfg.setQueryParallelism(PARRALEL_LEVEL);

            try {
                // Create caches.
                ignite.getOrCreateCache(entityACacheCfg);
                ignite.getOrCreateCache(entityBCacheCfg);
                ignite.getOrCreateCache(entityCCacheCfg);
                ignite.getOrCreateCache(entityDCacheCfg);

                // Populate caches.
                System.out.println(">>> Populate Caches.");
                initialize();

                sqlQueryWithJoinEntityAEntityB();
                sqlQueryWithJoinEntityCEntityD();
                sqlQueryWithJoinEntityAEntityBWithEntityCEntityD();

                System.out.println(">>> Stopping.");
            } finally {
                // Distributed cache could be removed from cluster only by Ignite.destroyCache() call.
                ignite.destroyCache(A_CACHE);
                ignite.destroyCache(B_CACHE);
                ignite.destroyCache(C_CACHE);
                ignite.destroyCache(D_CACHE);
            }

            print("SQL queries example finished.");
        }
    }

    private static void sqlQueryWithJoinEntityAEntityB() {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);

        String joinSql = "select count(1) from \"EntityA\".EntityA as a inner join \"EntityB\".EntityB as b on a.id = b.entityA ";
        SqlFieldsQuery q1 = new SqlFieldsQuery(joinSql);
        q1.setDistributedJoins(true);
        q1.setPageSize(100);
        long t = System.currentTimeMillis();
        FieldsQueryCursor<List<?>> c = cacheA.query(q1);
        List<List<?>> l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A --> B with distributed joins with h2 with join", l, t);

        
        

        SqlFieldsQuery q2 = new SqlFieldsQuery(joinSql);
        q2.setDistributedJoins(false);
        q2.setPageSize(100);

        
        int[] partitions = Ignition.ignite().affinity(A_CACHE).primaryPartitions(Ignition.ignite().cluster().forLocal().node());
        int[][] p = partition(partitions, 1024 / PARRALEL_LEVEL);

        t = System.currentTimeMillis();
        System.out.println(p.length);
        for (int[] parts : p) {
            q2.setPartitions(parts);
            c = cacheA.query(q2);              
            c.spliterator().tryAdvance(w -> { System.out.println("************ w: " + w); });
        }
        t = System.currentTimeMillis() - t;
//        print("All Entity A --> B w/o distributed joins with h2 with join", l, t);

        
        
        joinSql = "select  /*+ QUERY_ENGINE('calcite') */ count(*) from \"EntityA\".EntityA as a inner join \"EntityB\".EntityB as b on a.id = b.entityA";
        q1 = new SqlFieldsQuery(joinSql);
        q1.setPageSize(100);
        q1.setDistributedJoins(true);
        t = System.currentTimeMillis();
        c = cacheA.query(q1);
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A --> B with distributed joins with calcite with join", l, t);

        q2 = new SqlFieldsQuery(joinSql);
        q2.setDistributedJoins(false);
        q2.setPageSize(100);
        t = System.currentTimeMillis();
        c = cacheA.query(q2);
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A --> B w/o distributed joins with calcite with join", l, t);

        joinSql = "select  /*+ QUERY_ENGINE('calcite') */ count(*) from \"EntityA\".EntityA as a,  \"EntityB\".EntityB as b where a.id = b.entityA  ";
        q1 = new SqlFieldsQuery(joinSql);
        q1.setDistributedJoins(true);
        q1.setPageSize(100);
        t = System.currentTimeMillis();
        c = cacheA.query(q1);
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A --> B with distributed joins with calcite with where", l, t);

    }

    private static void sqlQueryWithJoinEntityCEntityD() {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);

        String joinSql = "select count(1) from \"EntityC\".EntityC as c inner join \"EntityD\".EntityD as d on c.id = d.entityC";
        SqlFieldsQuery q1 = new SqlFieldsQuery(joinSql);
        q1.setDistributedJoins(true);
        q1.setPageSize(100);
        long t = System.currentTimeMillis();
        FieldsQueryCursor<List<?>> c = cacheA.query(q1);
        List<List<?>> l = cacheA.query(q1).getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity C --> D with distributed joins", l, t);

        SqlFieldsQuery q2 = new SqlFieldsQuery(joinSql);
        q2.setDistributedJoins(false);
        q2.setPageSize(100);
        t = System.currentTimeMillis();
        c = cacheA.query(q2);
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity C --> D w/o distributed joins", l, t);

        joinSql = "select /*+ QUERY_ENGINE('calcite') */  count(*) from \"EntityC\".EntityC as c " + "inner join \"EntityD\".EntityD as d on c.id = d.entityC";
        q1 = new SqlFieldsQuery(joinSql);
        q1.setPageSize(100);
        q1.setDistributedJoins(true);
        t = System.currentTimeMillis();
        c = cacheA.query(q1);
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity C --> D with distributed joins with calcite", l, t);

        q2 = new SqlFieldsQuery(joinSql);
        q2.setDistributedJoins(false);
        q2.setPageSize(100);
        t = System.currentTimeMillis();
        c = cacheA.query(q2);
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity C --> D w/o distributed joins with calcite", l, t);

    }

    private static void sqlQueryWithJoinEntityAEntityBWithEntityCEntityD() {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);

        String joinSql = "select count(1) from \"EntityA\".EntityA as a " + " inner join \"EntityB\".EntityB as b on a.id = b.entityA" + " inner join \"EntityC\".EntityC as c on a.entityC = c.id"
                + " inner join \"EntityD\".EntityD as d on c.id = d.entityC";
        SqlFieldsQuery q1 = new SqlFieldsQuery(joinSql);
        q1.setDistributedJoins(true);
        q1.setPageSize(100);
        FieldsQueryCursor<List<?>> c = cacheA.query(q1);
        long t = System.currentTimeMillis();
        List<List<?>> l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A (--> B) --> C (-->D) with distributed joins with H2", l, t);

        SqlFieldsQuery q2 = new SqlFieldsQuery(joinSql);
        q2.setDistributedJoins(false);
        q2.setPageSize(100);
        c = cacheA.query(q2);
        t = System.currentTimeMillis();
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A (--> B) --> C (-->D) w/o distributed joins with H2", l, t);

        joinSql = "select  /*+ QUERY_ENGINE('calcite') */  count(*) from \"EntityA\".EntityA as a " + " inner join \"EntityB\".EntityB as b on a.id = b.entityA"
                + " inner join \"EntityC\".EntityC as c on a.entityC = c.id" + " inner join \"EntityD\".EntityD as d on c.id = d.entityC";
        q1 = new SqlFieldsQuery(joinSql);
        q1.setDistributedJoins(true);
        q2.setPageSize(100);
        c = cacheA.query(q1);
        t = System.currentTimeMillis();
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A (--> B) --> C (-->D) with distributed joins with calcite", l, t);

        q2 = new SqlFieldsQuery(joinSql);
        q2.setDistributedJoins(false);
        q2.setPageSize(100);
        c = cacheA.query(q2);
        t = System.currentTimeMillis();
        l = c.getAll();
        t = System.currentTimeMillis() - t;
        print("All Entity A (--> B) --> C (-->D) w/o distributed joins with calcite", l, t);

    }

    /**
     * Populate cache with test data.
     */
    private static void initialize() {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);
        IgniteCache<EntityBKey, EntityB> cacheB = Ignition.ignite().cache(B_CACHE);

        for (long i = 0; i < NB_ENTITY_A; i++) {
            EntityAKey ka = new EntityAKey(Long.toString(i));
            EntityA va = new EntityA(Long.toString(i), "entityA-value-" + Long.toString(i), Long.toString(i % NB_ENTITY_C));
            cacheA.put(ka, va);

            for (long j = 0; j < NB_ENTITY_B; j++) {

                EntityBKey kb = new EntityBKey(Long.toString(j), Long.toString(i));
                EntityB vb = new EntityB(Long.toString(j), Long.toString(i), "entityB-value-" + Long.toString(j));
                cacheB.put(kb, vb);
            }
        }

        IgniteCache<EntityCKey, EntityC> cacheC = Ignition.ignite().cache(C_CACHE);
        IgniteCache<EntityDKey, EntityD> cacheD = Ignition.ignite().cache(D_CACHE);

        for (long i = 0; i < NB_ENTITY_C; i++) {
            EntityCKey kc = new EntityCKey(Long.toString(i));
            EntityC vc = new EntityC(Long.toString(i), "entityC-value-" + Long.toString(i));
            cacheC.put(kc, vc);

            for (long j = 0; j < NB_ENTITY_D; j++) {

                EntityDKey kd = new EntityDKey(Long.toString(j), Long.toString(i));
                EntityD vd = new EntityD(Long.toString(j), Long.toString(i), "entityD-value-" + Long.toString(j));
                cacheD.put(kd, vd);
            }
        }
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col, long t) {
        print(msg + " : " + t + " ms");
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }
    
    
    private static int[][] partition(int[] array, int partitionSize) {
        Objects.requireNonNull(array, "array must not be null");
        int q = array.length / partitionSize;
        int r = array.length % partitionSize;
        int[][] ret = new int[r > 0 ? q + 1 : q][];
        for (int i = 0; i < q; i++) {
            ret[i] = Arrays.copyOfRange(array, partitionSize * i, partitionSize * (1 + i));
        }
        if (r > 0) {
            ret[q] = Arrays.copyOfRange(array, partitionSize * q, partitionSize * q + r);
        }
        return ret;
    }

}
