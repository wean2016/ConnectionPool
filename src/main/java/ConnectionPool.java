// Copyright (c) 2007 Keith D Gregory

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Manages a pool of database connections, using phantom references to
 * return the connections to the pool if the caller forgets. To make
 * this work, we use a {@link PooledConnection}, which wraps the actual
 * connection and delegates most operations. The user is given this
 * object, and we maintain a phantom reference to it, along with a
 * strong reference to the actual connection. When the phantom gets
 * queued, we return the actual connection to the pool.
 * <p>
 * Since this is demo code, we create the maximum number of connections
 * when the pool is constructed, and don't try to regenerate connections
 * that get closed.
 */
public class ConnectionPool {
    private final Queue<Connection> _pool = new LinkedList<Connection>();

    private ReferenceQueue<Object> _refQueue = new ReferenceQueue<Object>();
    private IdentityHashMap<Object, Connection> _ref2Cxt = new IdentityHashMap<Object, Connection>();
    private IdentityHashMap<Connection, Object> _cxt2Ref = new IdentityHashMap<Connection, Object>();


    /**
     * @param driver  The JDBC driver class to use for connections.
     * @param url     The URL used to connect to the database.
     * @param user    The database user's name.
     * @param passwd  The database user's password.
     * @param maxConn Maximum connections allowed through this pool.
     * @throws RuntimeException, with the original exception as cause,
     *                           if unable to construct pool for any reason
     */
    public ConnectionPool(String driver, String url, String user, String passwd, int maxConn) {
        try {
            Class.forName(driver);
            for (int ii = 0; ii < maxConn; ii++) {
                _pool.add(DriverManager.getConnection(url, user, passwd));
            }
        } catch (Exception e) {
            throw new RuntimeException("unable to initialize", e);
        }
    }


    /**
     * The internal method to retrieve a connection from the pool,
     * associating it with a weak reference. This is called from
     * {@link #getConnection}, which is responsible for ensuring
     * that there's a connection in the pool.
     */
    private synchronized Connection wrapConnection(Connection cxt) {
        Connection wrapped = PooledConnection.newInstance(this, cxt);
        PhantomReference<Connection> ref = new PhantomReference<Connection>(wrapped, _refQueue);
        _cxt2Ref.put(cxt, ref);
        _ref2Cxt.put(ref, cxt);
        System.err.println("Acquired connection " + cxt);
        return wrapped;
    }


    /**
     * Returns a connection to the pool. This is meant to be called by
     * {@link PooledConnection} and nothing else, so package protected.
     */
    synchronized void releaseConnection(Connection cxt) {
        Object ref = _cxt2Ref.remove(cxt);
        _ref2Cxt.remove(ref);
        _pool.offer(cxt);
        System.err.println("Released connection " + cxt);
    }


    /**
     * Returns a connection to the pool when the associated reference is
     * enqueued.
     */
    private synchronized void releaseConnection(Reference<?> ref) {
        Connection cxt = _ref2Cxt.remove(ref);
        if (cxt != null)
            releaseConnection(cxt);
    }


    /**
     * Called by {@link #getConnection} when there are no connections in the
     * pool, to see if one has been recovered by the garbage collector. This
     * function waits a short time, but then returns so that the caller can
     * again look in the pool.
     */
    private void tryWaitingForGarbageCollector() {
        try {
            Reference<?> ref = _refQueue.remove(100);
            if (ref != null)
                releaseConnection(ref);
        } catch (InterruptedException ignored) {
            // we have to catch this exception, but it provides no information here
            // a production-quality pool might use it as part of an orderly shutdown
        }
    }


//----------------------------------------------------------------------------
//  Public Methods
//----------------------------------------------------------------------------

    /**
     * Retrieves a connection from the pool, blocking until one becomes
     * available.
     */
    public Connection getConnection()
            throws SQLException {
        while (true) {
            synchronized (this) {
                if (_pool.size() > 0)
                    return wrapConnection(_pool.remove());
            }

            tryWaitingForGarbageCollector();
        }
    }
}