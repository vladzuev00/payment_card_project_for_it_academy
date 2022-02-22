package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool;

import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.exception.DataBaseConnectionPoolAccessConnectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.exception.DataBaseConnectionPoolFullingException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.*;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.databaseproperty.DataBaseProperty.*;

public final class DataBaseConnectionPool implements AutoCloseable
{
    private final Future<BlockingQueue<Connection>> holderOfAvailableConnections;

    private DataBaseConnectionPool(final Future<BlockingQueue<Connection>> holderOfAvailableConnections)
    {
        super();
        this.holderOfAvailableConnections = holderOfAvailableConnections;
    }

    public static DataBaseConnectionPool createDataBaseConnectionPool()
    {
        if(DataBaseConnectionPool.dataBaseConnectionPool == null)
        {
            synchronized(DataBaseConnectionPool.class)
            {
                if(DataBaseConnectionPool.dataBaseConnectionPool == null)
                {
                    final ExecutorService executorService = Executors.newSingleThreadExecutor();
                    final PoolFuller poolFuller = new PoolFuller(
                            URL.getValue(), NAME_OF_USER.getValue(), PASSWORD.getValue());
                    final Future<BlockingQueue<Connection>> holderOfAvailableConnections
                            = executorService.submit(poolFuller);
                    executorService.shutdown();
                    DataBaseConnectionPool.dataBaseConnectionPool
                            = new DataBaseConnectionPool(holderOfAvailableConnections);
                }
            }
        }
        return DataBaseConnectionPool.dataBaseConnectionPool;
    }

    private static DataBaseConnectionPool dataBaseConnectionPool;
    private static final int AMOUNT_OF_INVOLVED_CONNECTIONS = 20;

    private static final class PoolFuller implements Callable<BlockingQueue<Connection>>
    {
        private final String urlOfDataBase;
        private final String nameOfUserOfDataBase;
        private final String passwordOfDataBase;

        public PoolFuller(final String urlOfDataBase, final String nameOfUserOfDataBase,
                          final String passwordOfDataBase)
        {
            super();
            this.urlOfDataBase = urlOfDataBase;
            this.nameOfUserOfDataBase = nameOfUserOfDataBase;
            this.passwordOfDataBase = passwordOfDataBase;
        }

        @Override
        public final BlockingQueue<Connection> call()
        {
            final BlockingQueue<Connection> fulledConnections = new ArrayBlockingQueue<Connection>(
                    DataBaseConnectionPool.AMOUNT_OF_INVOLVED_CONNECTIONS);
            try
            {
                Connection currentFulledConnection;
                for(int i = 0; i < DataBaseConnectionPool.AMOUNT_OF_INVOLVED_CONNECTIONS; i++)
                {
                    currentFulledConnection = DriverManager.getConnection(this.urlOfDataBase, this.nameOfUserOfDataBase,
                            this.passwordOfDataBase);
                    fulledConnections.add(currentFulledConnection);
                }
                return fulledConnections;
            }
            catch(final SQLException cause)
            {
                final DataBaseConnectionPoolFullingException mainException
                        = new DataBaseConnectionPoolFullingException();
                if(!fulledConnections.isEmpty())
                {
                    try
                    {
                        for(final Connection fulledConnection : fulledConnections)
                        {
                            fulledConnection.close();
                        }
                    }
                    catch(final SQLException exceptionOfClosingConnection)
                    {
                        mainException.addSuppressed(exceptionOfClosingConnection);
                    }
                }
                throw mainException;
            }
        }
    }

    public final int getAmountOfAvailableConnections()
    {
        try
        {
            final BlockingQueue<Connection> availableConnections = this.holderOfAvailableConnections.get();
            return availableConnections.size();
        }
        catch(final ExecutionException | InterruptedException cause)
        {
            throw new DataBaseConnectionPoolFullingException(cause);
        }
    }

    public final Connection findAvailableConnection()
            throws DataBaseConnectionPoolAccessConnectionException
    {
        try
        {
            final BlockingQueue<Connection> availableConnection = this.holderOfAvailableConnections.get();
            final Connection foundConnection = availableConnection.poll(
                    DataBaseConnectionPool.AMOUNT_OF_UNITS_OF_WAITING_OF_CONNECTION,
                    DataBaseConnectionPool.TIME_UNIT_OF_WAITING_OF_CONNECTION);
            if(foundConnection == null)
            {
                throw new DataBaseConnectionPoolAccessConnectionException("Trying of getting connection from connection"
                        + " pool is very long.");
            }
            return foundConnection;
        }
        catch(final ExecutionException cause)
        {
            throw new DataBaseConnectionPoolFullingException(cause);
        }
        catch(final InterruptedException cause)
        {
            throw new DataBaseConnectionPoolAccessConnectionException(cause);
        }
    }

    private static final long AMOUNT_OF_UNITS_OF_WAITING_OF_CONNECTION = 5;
    private static final TimeUnit TIME_UNIT_OF_WAITING_OF_CONNECTION = TimeUnit.SECONDS;

    public final void returnConnectionToPool(final Connection returnedConnection)
    {
        try
        {
            final BlockingQueue<Connection> availableConnections = this.holderOfAvailableConnections.get();
            availableConnections.add(returnedConnection);
        }
        catch(final InterruptedException | ExecutionException cause)
        {
            throw new DataBaseConnectionPoolFullingException(cause);
        }
    }

    @Override
    public final void close() throws IOException
    {
        try
        {
            final BlockingQueue<Connection> availableConnections = this.holderOfAvailableConnections.get();
            for(final Connection closedConnection : availableConnections)
            {
                closedConnection.close();
            }
        }
        catch(final ExecutionException | InterruptedException cause)
        {
            throw new DataBaseConnectionPoolFullingException(cause);
        }
        catch(final SQLException cause)
        {
            throw new IOException(cause);
        }
    }
}