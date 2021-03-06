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

package org.gridgain.examples.datagrid.store.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.examples.datagrid.store.*;
import org.gridgain.grid.cache.store.*;
import org.hibernate.*;
import org.hibernate.cfg.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Example of {@link GridCacheStore} implementation that uses Hibernate
 * and deals with maps {@link UUID} to {@link Person}.
 */
public class CacheHibernatePersonStore extends GridCacheStoreAdapter<Long, Person> {
    /** Default hibernate configuration resource path. */
    private static final String DFLT_HIBERNATE_CFG = "/org/gridgain/examples/datagrid/store/hibernate/hibernate.cfg.xml";

    /** Session attribute name. */
    private static final String ATTR_SES = "HIBERNATE_STORE_SESSION";

    /** Session factory. */
    private SessionFactory sesFactory;

    /**
     * Default constructor.
     */
    public CacheHibernatePersonStore() {
        sesFactory = new Configuration().configure(DFLT_HIBERNATE_CFG).buildSessionFactory();
    }

    /** {@inheritDoc} */
    @Override public Person load(@Nullable IgniteTx tx, Long key) throws IgniteCheckedException {
        System.out.println(">>> Store load [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Session ses = session(tx);

        try {
            return (Person) ses.get(Person.class, key);
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new IgniteCheckedException("Failed to load value from cache store with key: " + key, e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, Long key, @Nullable Person val)
        throws IgniteCheckedException {
        System.out.println(">>> Store put [key=" + key + ", val=" + val + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        if (val == null) {
            remove(tx, key);

            return;
        }

        Session ses = session(tx);

        try {
            ses.saveOrUpdate(val);
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new IgniteCheckedException("Failed to put value to cache store [key=" + key + ", val" + val + "]", e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"JpaQueryApiInspection"})
    @Override public void remove(@Nullable IgniteTx tx, Long key) throws IgniteCheckedException {
        System.out.println(">>> Store remove [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Session ses = session(tx);

        try {
            ses.createQuery("delete " + Person.class.getSimpleName() + " where key = :key")
                .setParameter("key", key).setFlushMode(FlushMode.ALWAYS).executeUpdate();
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new IgniteCheckedException("Failed to remove value from cache store with key: " + key, e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) throws IgniteCheckedException {
        if (args == null || args.length == 0 || args[0] == null)
            throw new IgniteCheckedException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        Session ses = session(null);

        try {
            int cnt = 0;

            List res = ses.createCriteria(Person.class).list();

            if (res != null) {
                Iterator iter = res.iterator();

                while (cnt < entryCnt && iter.hasNext()) {
                    Person person = (Person)iter.next();

                    clo.apply(person.getId(), person);

                    cnt++;
                }
            }

            System.out.println(">>> Loaded " + cnt + " values into cache.");
        }
        catch (HibernateException e) {
            throw new IgniteCheckedException("Failed to load values from cache store.", e);
        }
        finally {
            end(ses, null);
        }
    }

    /**
     * Rolls back hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private void rollback(Session ses, IgniteTx tx) {
        // Rollback only if there is no cache transaction,
        // otherwise txEnd() will do all required work.
        if (tx == null) {
            Transaction hTx = ses.getTransaction();

            if (hTx != null && hTx.isActive())
                hTx.rollback();
        }
    }

    /**
     * Ends hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private void end(Session ses, @Nullable IgniteTx tx) {
        // Commit only if there is no cache transaction,
        // otherwise txEnd() will do all required work.
        if (tx == null) {
            Transaction hTx = ses.getTransaction();

            if (hTx != null && hTx.isActive())
                hTx.commit();

            ses.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        Session ses = tx.removeMeta(ATTR_SES);

        if (ses != null) {
            Transaction hTx = ses.getTransaction();

            if (hTx != null) {
                try {
                    if (commit) {
                        ses.flush();

                        hTx.commit();
                    }
                    else
                        hTx.rollback();

                    System.out.println("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
                }
                catch (HibernateException e) {
                    throw new IgniteCheckedException("Failed to end transaction [xid=" + tx.xid() +
                        ", commit=" + commit + ']', e);
                }
                finally {
                    ses.close();
                }
            }
        }
    }

    /**
     * Gets Hibernate session.
     *
     * @param tx Cache transaction.
     * @return Session.
     */
    private Session session(@Nullable IgniteTx tx) {
        Session ses;

        if (tx != null) {
            ses = tx.meta(ATTR_SES);

            if (ses == null) {
                ses = sesFactory.openSession();

                ses.beginTransaction();

                // Store session in transaction metadata, so it can be accessed
                // for other operations on the same transaction.
                tx.addMeta(ATTR_SES, ses);

                System.out.println("Hibernate session open [ses=" + ses + ", tx=" + tx.xid() + "]");
            }
        }
        else {
            ses = sesFactory.openSession();

            ses.beginTransaction();
        }

        return ses;
    }
}
