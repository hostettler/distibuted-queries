package org.mytest.poc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private static final int PARRALEL_LEVEL = 8;
    private static final int NB_ENTITY_A = PARRALEL_LEVEL * 50_000;
    private static final int NB_ENTITY_B = PARRALEL_LEVEL * 5;
    private static final int NB_ENTITY_C = PARRALEL_LEVEL * 1_000;
    private static final int NB_ENTITY_D = PARRALEL_LEVEL * 10;

    public static void main(String[] args) throws IgniteException, FileNotFoundException, InterruptedException, ExecutionException {
        try (Ignite ignite = Ignition.start(new FileInputStream("config/ignite-config.xml"))) {
            System.out.println();
            System.out.println(">>> Run Queries with affinities");

            createAndPopulateCaches(ignite, true);
            sqlQueryWithJoinEntityAEntityB(true);
            sqlQueryWithJoinEntityCEntityD(true);
            sqlQueryWithJoinEntityAEntityBWithEntityCEntityD(true);

            ignite.destroyCache(A_CACHE);
            ignite.destroyCache(B_CACHE);
            ignite.destroyCache(C_CACHE);
            ignite.destroyCache(D_CACHE);

            System.out.println(">>> Run Queries without affinities");
            createAndPopulateCaches(ignite, false);
            sqlQueryWithJoinEntityAEntityB(false);
            sqlQueryWithJoinEntityCEntityD(false);
            sqlQueryWithJoinEntityAEntityBWithEntityCEntityD(false);

            ignite.destroyCache(A_CACHE);
            ignite.destroyCache(B_CACHE);
            ignite.destroyCache(C_CACHE);
            ignite.destroyCache(D_CACHE);

            System.out.println(">>> Stopping.");

        } finally {
            Ignition.stop(true);
        }
    }

    private static void createAndPopulateCaches(Ignite ignite, boolean affinity) throws InterruptedException, ExecutionException {

        // ----------------------------------------------------------------------------------------
        CacheConfiguration<EntityAKey, EntityA> entityACacheCfg = new CacheConfiguration<>(A_CACHE);
        entityACacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        QueryEntity queryEntity = new QueryEntity();
        queryEntity.addQueryField("id", String.class.getName(), null);
        queryEntity.addQueryField("entityC", String.class.getName(), null);
        queryEntity.setTableName(EntityA.class.getSimpleName());
        queryEntity.setKeyType(EntityAKey.class.getCanonicalName());
        queryEntity.setValueType(EntityA.class.getCanonicalName());
        Set<String> keyFields = Set.of("id");
        queryEntity.setKeyFields(keyFields);
        Collection<QueryIndex> indexedColumns = new ArrayList<>();
        indexedColumns.add(new QueryIndex("id"));
        indexedColumns.add(new QueryIndex("entityC"));
        queryEntity.setIndexes(indexedColumns);
        if (affinity) {
            entityACacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityAKey.class).setAffinityKeyFieldName("id"));
        }
        entityACacheCfg.setQueryEntities(Set.of(queryEntity));
        entityACacheCfg.setIndexedTypes(EntityAKey.class, EntityA.class);
        entityACacheCfg.setQueryParallelism(PARRALEL_LEVEL);

        // ----------------------------------------------------------------------------------------
        CacheConfiguration<EntityBKey, EntityB> entityBCacheCfg = new CacheConfiguration<>(B_CACHE);
        entityBCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        queryEntity = new QueryEntity();
        queryEntity.addQueryField("id", String.class.getName(), null);
        queryEntity.addQueryField("entityA", String.class.getName(), null);
        queryEntity.setTableName(EntityB.class.getSimpleName());
        queryEntity.setKeyType(EntityBKey.class.getCanonicalName());
        queryEntity.setValueType(EntityB.class.getCanonicalName());
        keyFields = Set.of("id", "entityA");
        queryEntity.setKeyFields(keyFields);
        indexedColumns = new ArrayList<>();
        indexedColumns.add(new QueryIndex("id"));
        indexedColumns.add(new QueryIndex("entityA"));
        queryEntity.setIndexes(indexedColumns);
        if (affinity) {
            entityBCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityBKey.class).setAffinityKeyFieldName("entityA"));
        }
        entityBCacheCfg.setQueryEntities(Set.of(queryEntity));
        entityBCacheCfg.setIndexedTypes(EntityBKey.class, EntityB.class);
        entityBCacheCfg.setQueryParallelism(PARRALEL_LEVEL);

        // ----------------------------------------------------------------------------------------
        CacheConfiguration<EntityCKey, EntityC> entityCCacheCfg = new CacheConfiguration<>(C_CACHE);
        entityCCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        queryEntity = new QueryEntity();
        queryEntity.addQueryField("id", String.class.getName(), null);
        queryEntity.setTableName(EntityC.class.getSimpleName());
        queryEntity.setKeyType(EntityCKey.class.getCanonicalName());
        queryEntity.setValueType(EntityC.class.getCanonicalName());
        keyFields = Set.of("id");
        queryEntity.setKeyFields(keyFields);
        indexedColumns = Collections.singleton(new QueryIndex("id"));
        queryEntity.setIndexes(indexedColumns);
        if (affinity) {
            entityCCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityCKey.class).setAffinityKeyFieldName("id"));
        }
        entityCCacheCfg.setQueryEntities(Set.of(queryEntity));
        entityCCacheCfg.setIndexedTypes(EntityCKey.class, EntityC.class);
        entityCCacheCfg.setQueryParallelism(PARRALEL_LEVEL);

        // ----------------------------------------------------------------------------------------
        CacheConfiguration<EntityDKey, EntityD> entityDCacheCfg = new CacheConfiguration<>(D_CACHE);
        entityDCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        queryEntity = new QueryEntity();
        queryEntity.addQueryField("id", String.class.getName(), null);
        queryEntity.addQueryField("entityC", String.class.getName(), null);
        queryEntity.setTableName(EntityD.class.getSimpleName());
        queryEntity.setKeyType(EntityDKey.class.getCanonicalName());
        queryEntity.setValueType(EntityD.class.getCanonicalName());
        keyFields = Set.of("id", "entityC");
        queryEntity.setKeyFields(keyFields);
        indexedColumns = new ArrayList<>();
        indexedColumns.add(new QueryIndex("id"));
        indexedColumns.add(new QueryIndex("entityC"));
        queryEntity.setIndexes(indexedColumns);
        if (affinity) {
            entityDCacheCfg.setKeyConfiguration(new CacheKeyConfiguration(EntityDKey.class).setAffinityKeyFieldName("entityC"));
        }
        entityDCacheCfg.setQueryEntities(Set.of(queryEntity));
        entityDCacheCfg.setIndexedTypes(EntityDKey.class, EntityD.class);
        entityDCacheCfg.setQueryParallelism(PARRALEL_LEVEL);

        ignite.getOrCreateCache(entityACacheCfg);
        ignite.getOrCreateCache(entityBCacheCfg);
        ignite.getOrCreateCache(entityCCacheCfg);
        ignite.getOrCreateCache(entityDCacheCfg);

        // Populate caches.
        System.out.println(">>> Populating Caches.");
        long t = System.currentTimeMillis();
        initialize();
        System.out.println(String.format(">>> Populated Caches in %,d ms", (System.currentTimeMillis() - t)));

    }

    private static void sqlQueryWithJoinEntityAEntityB(boolean useAffinity) {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);

        String joinSql = "count(1) from \"EntityA\".EntityA as a inner join \"EntityB\".EntityB as b on a.id = b.entityA ";
        executeQuery(cacheA, joinSql, useAffinity, true, true, "A --> B");
        executeQuery(cacheA, joinSql, useAffinity, false, true, "A --> B");
        executeQuery(cacheA, joinSql, useAffinity, true, false, "A --> B");
        executeQuery(cacheA, joinSql, useAffinity, false, false, "A --> B");
    }

    private static void sqlQueryWithJoinEntityCEntityD(boolean useAffinity) {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);

        String joinSql = "count(1) from \"EntityC\".EntityC as c inner join \"EntityD\".EntityD as d on c.id = d.entityC";
        executeQuery(cacheA, joinSql, useAffinity, true, true, "C --> D");
        executeQuery(cacheA, joinSql, useAffinity, false, true, "C --> D");
        executeQuery(cacheA, joinSql, useAffinity, true, false, "C --> D");
        executeQuery(cacheA, joinSql, useAffinity, false, false, "C --> D");
    }

    private static void sqlQueryWithJoinEntityAEntityBWithEntityCEntityD(boolean useAffinity) {
        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);

        String joinSql = "count(1) from \"EntityA\".EntityA as a " + " left outer join \"EntityB\".EntityB as b on a.id = b.entityA"
                + " left outer join \"EntityC\".EntityC as c on a.entityC = c.id" + " left outer join \"EntityD\".EntityD as d on d.entityC = a.entityC";

        executeQuery(cacheA, joinSql, useAffinity, true, true, "(A --> B) --> (C --> D)");
        executeQuery(cacheA, joinSql, useAffinity, false, true, "(A --> B) --> (C --> D)");
        executeQuery(cacheA, joinSql, useAffinity, true, false, "(A --> B) --> (C --> D)");
        executeQuery(cacheA, joinSql, useAffinity, false, false, "(A --> B) --> (C --> D)");
    }

    private static void executeQuery(IgniteCache<EntityAKey, EntityA> cacheA, String joinSql, boolean useAffinity, boolean distributedJoins, boolean isCalcite, String label) {
        
        joinSql = "select " + (isCalcite ? " /*+ QUERY_ENGINE('calcite') */ " : "") + joinSql;
        SqlFieldsQuery q1 = new SqlFieldsQuery(joinSql);
        q1.setDistributedJoins(distributedJoins);
        q1.setPageSize(100);
        long t = System.currentTimeMillis();
        FieldsQueryCursor<List<?>> c = cacheA.query(q1);
        List<List<?>> l = c.getAll();
        t = System.currentTimeMillis() - t;
        print(String.format("Count %s = %,d with %s joins and %s using %s engine in %,d ms", label, l.get(0).get(0), distributedJoins ? "distributed" : "non-distributed",
                useAffinity ? "with affinity" : "without affinity", isCalcite ? "calcite" : " h2 ", t));
    }

    /**
     * Populate cache with test data.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static void initialize() throws InterruptedException, ExecutionException {

        ExecutorService executorService = new ThreadPoolExecutor(PARRALEL_LEVEL, PARRALEL_LEVEL, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        IgniteCache<EntityAKey, EntityA> cacheA = Ignition.ignite().cache(A_CACHE);
        IgniteCache<EntityBKey, EntityB> cacheB = Ignition.ignite().cache(B_CACHE);

        System.out.println(String.format("card(entityA)=%d size(blockA)=%d", NB_ENTITY_A, (NB_ENTITY_A / PARRALEL_LEVEL)));

        List<Callable<String>> callableTasks = new ArrayList<>();
        final long blockSizeA = (NB_ENTITY_A / PARRALEL_LEVEL);
        for (long k = 0; k < PARRALEL_LEVEL; k++) {
            final long step = k;
            System.out.println(String.format("Prepare Callable for A step %d for %d to %d", step, step * blockSizeA, (step + 1) * blockSizeA));
            callableTasks.add(() -> {
                System.out.println(String.format("Callable for A step %d for %d to %d", step, step * blockSizeA, (step + 1) * blockSizeA));
                for (long i = step * blockSizeA; i < ((step + 1) * blockSizeA) && i < NB_ENTITY_A; i++) {
                    EntityAKey ka = new EntityAKey("AKey-" + Long.toString(i));
                    EntityA va = new EntityA("AKey-" + Long.toString(i), "entityA-value-" + Long.toString(i), "CKey-" + Long.toString(i % NB_ENTITY_C));
                    cacheA.put(ka, va);

                    for (long j = 0; j < NB_ENTITY_B; j++) {

                        EntityBKey kb = new EntityBKey("BKey-" + Long.toString(j), "AKey-" + Long.toString(i));
                        EntityB vb = new EntityB("BKey-" + Long.toString(j), "AKey-" + Long.toString(i), "entityB-value-" + Long.toString(j));
                        cacheB.put(kb, vb);
                    }
                }
                return "success";
            });

        }
        executorService.invokeAll(callableTasks);

        IgniteCache<EntityCKey, EntityC> cacheC = Ignition.ignite().cache(C_CACHE);
        IgniteCache<EntityDKey, EntityD> cacheD = Ignition.ignite().cache(D_CACHE);

        System.out.println(String.format("card(entityC)=%d size(blockC)=%d", NB_ENTITY_C, (NB_ENTITY_C / PARRALEL_LEVEL)));
        final long blockSizeC = (NB_ENTITY_C / PARRALEL_LEVEL);
        callableTasks.clear();
        for (long k = 0; k < PARRALEL_LEVEL; k++) {
            final long step = k;
            System.out.println(String.format("Prepare Callable for C step %d for %d to %d", step, step * blockSizeC, (step + 1) * blockSizeC));
            callableTasks.add(() -> {
                System.out.println(String.format("Callable for C step %d for %d to %d", step, (step * blockSizeC), (step + 1) * blockSizeC));
                for (long i = step * (blockSizeC); i < (step + 1) * blockSizeC && i < NB_ENTITY_C; i++) {
                    EntityCKey kc = new EntityCKey("CKey-" + Long.toString(i));
                    EntityC vc = new EntityC("CKey-" + Long.toString(i), "entityC-value-" + Long.toString(i));
                    cacheC.put(kc, vc);

                    for (long j = 0; j < NB_ENTITY_D; j++) {

                        EntityDKey kd = new EntityDKey("DKey-" + Long.toString(j), "CKey-" + Long.toString(i));
                        EntityD vd = new EntityD("DKey-" + Long.toString(j), "CKey-" + Long.toString(i), "entityD-value-" + Long.toString(j));
                        cacheD.put(kd, vd);
                    }
                }
                return "success";
            });
        }
        executorService.invokeAll(callableTasks);
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

}
