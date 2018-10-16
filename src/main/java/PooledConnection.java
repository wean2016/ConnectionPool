// Copyright (c) 2007 Keith D Gregory

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;


/**
 * A wrapper for a JDBC database connection that is associated with a pool.
 * Closing this connection returns the wrapped connection to the pool, to
 * be used again.
 * <p>
 * This class is implemented as a reflection proxy invocation handler. There
 * are two reasons for doing this. First, most of the methods simply delegate
 * to an underlying connection; we'd have to write a lot of boilerplate for a
 * full <code>Connection</code> implementation. Second, and more important,
 * the <code>Connection</code> interface has changed over time, in ways that
 * are neither forward nor backward compatible. A concrete implementation is
 * tied to a single JDK version, which doesn't work well for example code that
 * might live for many years.
 * <p>
 * Given point #2, I would almost certainly choose a similar implementation in
 * a production pool. <ethods in this class are called relatively infrequently,
 * so the overhead of reflection is not important.
 */
public class PooledConnection
        implements InvocationHandler {
    private ConnectionPool _pool;
    private Connection _cxt;


    public PooledConnection(ConnectionPool pool, Connection cxt) {
        _pool = pool;
        _cxt = cxt;
    }


    private Connection getConnection() {
        try {
            if ((_cxt == null) || _cxt.isClosed())
                throw new RuntimeException("Connection is closed");
        } catch (SQLException ex) {
            throw new RuntimeException("unable to determine if underlying connection is open", ex);
        }

        return _cxt;
    }


//----------------------------------------------------------------------------
//  Reflection Proxy
//----------------------------------------------------------------------------

    /**
     * Factory to create reflection proxy.
     */
    public static Connection newInstance(ConnectionPool pool, Connection cxt) {
        return (Connection) Proxy.newProxyInstance(
                PooledConnection.class.getClassLoader(),
                new Class[]{Connection.class},
                new PooledConnection(pool, cxt));
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        try {
            if (method.getName().equals("close")) {
                close();
                return null;
            } else if (method.getName().equals("isClosed")) {
                return Boolean.valueOf(isClosed());
            } else {
                return method.invoke(getConnection(), args);
            }
        } catch (Throwable ex) {
            if (ex instanceof InvocationTargetException) {
                ex = ((InvocationTargetException) ex).getTargetException();
            }

            if ((ex instanceof Error) || (ex instanceof RuntimeException) || (ex instanceof SQLException)) {
                throw ex;
            }

            // it's a checked exception, almost certainly reflection-related;
            // we need to wrap for consumption
            throw new RuntimeException("exception during reflective invocation", ex);
        }
    }


//----------------------------------------------------------------------------
//  Connection methods that aren't simply delegates
//----------------------------------------------------------------------------

    private void close() throws SQLException {
        if (_cxt != null) {
            _pool.releaseConnection(_cxt);
            _cxt = null;
        }
    }


    private boolean isClosed() throws SQLException {
        return (_cxt == null) || (_cxt.isClosed());
    }
}