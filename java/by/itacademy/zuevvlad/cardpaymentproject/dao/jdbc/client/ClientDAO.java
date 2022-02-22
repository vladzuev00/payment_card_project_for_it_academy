package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.client;

import by.itacademy.zuevvlad.cardpaymentproject.dao.DAO;
import by.itacademy.zuevvlad.cardpaymentproject.dao.exception.*;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.DataBaseConnectionPool;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.exception.DataBaseConnectionPoolAccessConnectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.FounderGeneratedIdByDataBase;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.exception.FindingGeneratedIdByDataBaseException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.client.ResultSetMapperToClients;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.BankAccountTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.ClientTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.entity.Client;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.sql.*;
import java.util.Optional;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty.*;
import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.ClientTableProperty.*;

public final class ClientDAO implements DAO<Client>
{
    private final DataBaseConnectionPool dataBaseConnectionPool;
    private final Cryptographer<String, String> cryptographer;
    private final ResultSetMapperToCollection<Client, Set<Client>> resultSetMapperToClients;
    private final ResultSetMapperToCollection.ResultSetRowMapperToEntity<Client> resultSetRowMapperToClient;

    public ClientDAO()
    {
        super();

        this.dataBaseConnectionPool = DataBaseConnectionPool.createDataBaseConnectionPool();
        this.cryptographer = StringToStringCryptographer.createCryptographer();
        this.resultSetMapperToClients = ResultSetMapperToClients.createResultSetMapperToClients();
        this.resultSetRowMapperToClient = this.resultSetMapperToClients.getResultSetRowMapperToEntity();
    }

    @Override
    public final void addEntity(final Client addedClient)
            throws AddingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try
            {
                connectionToDataBase.setAutoCommit(false);
                try
                {
                    this.addInTableOfUsers(addedClient, connectionToDataBase);
                    this.addInTableOfClients(addedClient, connectionToDataBase);
                    connectionToDataBase.commit();
                }
                catch(final AddingEntityException exception)
                {
                    connectionToDataBase.rollback();
                    throw exception;
                }
            }
            finally
            {
                if(!connectionToDataBase.getAutoCommit())
                {
                    connectionToDataBase.setAutoCommit(true);
                }
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException cause)
        {
            throw new AddingEntityException(cause);
        }
    }

    private void addInTableOfUsers(final Client addedClient, final Connection connectionToDataBase)
            throws AddingEntityException
    {
        try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                ClientSqlOperation.PREPARED_STATEMENT_TO_INSERT_USER.getSqlQuery()))
        {
            preparedStatement.setString(
                    ClientSqlOperation.PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_INSERT_USER,
                    addedClient.getEmail());

            final String encryptedPassword = this.cryptographer.encrypt(addedClient.getPassword());
            preparedStatement.setString(
                    ClientSqlOperation.PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_INSERT_USER,
                    encryptedPassword);

            preparedStatement.executeUpdate();

            final FounderGeneratedIdByDataBase founderGeneratedIdByDataBase
                    = FounderGeneratedIdByDataBase.createFounderGeneratedIdByDataBase();
            final long generatedId = founderGeneratedIdByDataBase.findGeneratedIdInLastInserting(preparedStatement,
                    UserTableProperty.NAME_OF_COLUMN_OF_ID.getValue());
            addedClient.setId(generatedId);
        }
        catch(final SQLException | FindingGeneratedIdByDataBaseException cause)
        {
            throw new AddingEntityException(cause);
        }
    }

    private void addInTableOfClients(final Client addedClient, final Connection connectionToDataBase)
            throws AddingEntityException
    {
        try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                ClientSqlOperation.PREPARED_STATEMENT_TO_INSERT_CLIENT.getSqlQuery()))
        {
            preparedStatement.setString(
                    ClientSqlOperation.PARAMETER_INDEX_OF_NAME_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT,
                    addedClient.getName());
            preparedStatement.setString(
                    ClientSqlOperation.PARAMETER_INDEX_OF_SURNAME_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT,
                    addedClient.getSurname());
            preparedStatement.setString(
                    ClientSqlOperation.PARAMETER_INDEX_OF_PATRONYMIC_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT,
                    addedClient.getPatronymic());
            preparedStatement.setString(
                    ClientSqlOperation.PARAMETER_INDEX_OF_PHONE_NUMBER_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT,
                    addedClient.getPhoneNumber());
            preparedStatement.setLong(
                    ClientSqlOperation.PARAMETER_INDEX_OF_BANK_ACCOUNT_ID_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT,
                    addedClient.getBankAccount().getId());
            preparedStatement.setLong(
                    ClientSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT,
                    addedClient.getId());

            preparedStatement.executeUpdate();
        }
        catch(final SQLException cause)
        {
            throw new AddingEntityException(cause);
        }
    }

    @Override
    public final Iterable<Client> offloadEntities()
            throws OffloadingEntitiesException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final Statement statement = connectionToDataBase.createStatement())
            {
                final ResultSet resultSet = statement.executeQuery(ClientSqlOperation.OFFLOAD_ALL_CLIENTS.getSqlQuery());
                return this.resultSetMapperToClients.map(resultSet);
            }
            finally
            {
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException
                | ResultSetMappingToCollectionException cause)
        {
            throw new OffloadingEntitiesException(cause);
        }
    }

    @Override
    public final Optional<Client> findEntityById(final long idOfFoundClient)
            throws FindingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    ClientSqlOperation.PREPARED_STATEMENT_TO_SELECT_CLIENT_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(
                        ClientSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_SELECT_BY_ID,
                        idOfFoundClient);

                final ResultSet resultSet = preparedStatement.executeQuery();
                return resultSet.next() ? Optional.of(this.resultSetRowMapperToClient.mapCurrentRow(resultSet))
                        : Optional.empty();
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException
                | ResultSetRowMappingToEntityException cause)
        {
            throw new FindingEntityException(cause);
        }
    }

    @Override
    public final void updateEntity(final Client updatedClient)
            throws UpdatingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    ClientSqlOperation.PREPARED_STATEMENT_TO_UPDATE_CLIENT.getSqlQuery()))
            {
                preparedStatement.setString(ClientSqlOperation.PARAMETER_INDEX_OF_NAME_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedClient.getName());
                preparedStatement.setString(
                        ClientSqlOperation.PARAMETER_INDEX_OF_SURNAME_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedClient.getSurname());
                preparedStatement.setString(
                        ClientSqlOperation.PARAMETER_INDEX_OF_PATRONYMIC_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedClient.getPatronymic());
                preparedStatement.setString(
                        ClientSqlOperation.PARAMETER_INDEX_OF_PHONE_NUMBER_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedClient.getPhoneNumber());
                preparedStatement.setLong(
                        ClientSqlOperation.PARAMETER_INDEX_OF_BANK_ACCOUNT_ID_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedClient.getBankAccount().getId());
                preparedStatement.setLong(ClientSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedClient.getId());

                preparedStatement.executeUpdate();
            }
            finally
            {
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException cause)
        {
            throw new UpdatingEntityException(cause);
        }
    }

    @Override
    public final void deleteEntityById(final long idOfDeletedClient)
            throws DeletingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    ClientSqlOperation.PREPARED_STATEMENT_TO_DELETE_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(ClientSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DELETE,
                        idOfDeletedClient);
                preparedStatement.executeUpdate();
            }
            finally
            {
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException cause)
        {
            throw new DeletingEntityException(cause);
        }
    }

    @Override
    public final boolean isEntityWithGivenIdExisting(final long idOfResearchClient)
            throws DefiningExistingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    ClientSqlOperation.PREPARED_STATEMENT_TO_DEFINE_EXISTING_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(
                        ClientSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DEFINE_EXISTING,
                        idOfResearchClient);
                final ResultSet resultSet = preparedStatement.executeQuery();
                return resultSet.next();
            }
            finally
            {
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException cause)
        {
            throw new DefiningExistingEntityException(cause);
        }
    }

    private static enum ClientSqlOperation
    {
        PREPARED_STATEMENT_TO_INSERT_USER("INSERT INTO " + UserTableProperty.NAME_OF_TABLE.getValue() + " ("
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue()
                + ") VALUES (?, ?)"),
        PREPARED_STATEMENT_TO_INSERT_CLIENT("INSERT INTO " + ClientTableProperty.NAME_OF_TABLE.getValue() + " ("
                + NAME_OF_COLUMN_OF_NAME.getValue() + ", " + NAME_OF_COLUMN_OF_SURNAME.getValue() + ", "
                + NAME_OF_COLUMN_OF_PATRONYMIC.getValue() + ", " + NAME_OF_COLUMN_OF_PHONE_NUMBER.getValue() + ", "
                + NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue() + ", "
                + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + ") VALUES(?, ?, ?, ?, ?, ?)"),
        /*
            SELECT users.id, email, encrypted_password, name, surname, patronymic, phone_number, bank_account_id
            FROM clients LEFT OUTER JOIN users
                ON users.id = clients.id
                LEFT OUTER JOIN bank_accounts
                    ON clients.bank_account_id = bank_accounts.id
         */
        OFFLOAD_ALL_CLIENTS("SELECT " + UserTableProperty.NAME_OF_TABLE.getValue() + "."
                + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue() + ", "
                + NAME_OF_COLUMN_OF_NAME.getValue() + ", " + NAME_OF_COLUMN_OF_SURNAME.getValue() + ", "
                + NAME_OF_COLUMN_OF_PATRONYMIC.getValue() + ", " + NAME_OF_COLUMN_OF_PHONE_NUMBER.getValue() + ", "
                + NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue() + " FROM " + ClientTableProperty.NAME_OF_TABLE.getValue()
                + " LEFT OUTER JOIN " + UserTableProperty.NAME_OF_TABLE.getValue() + " ON "
                + ClientTableProperty.NAME_OF_TABLE.getValue() + "." + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue()
                + " = " + UserTableProperty.NAME_OF_TABLE.getValue() + "." + UserTableProperty.NAME_OF_COLUMN_OF_ID.getValue()
                + " LEFT OUTER JOIN " + BankAccountTableProperty.NAME_OF_TABLE.getValue() + " ON "
                + ClientTableProperty.NAME_OF_TABLE.getValue() + "." + NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue()
                + " = " + BankAccountTableProperty.NAME_OF_TABLE.getValue() + "."
                + BankAccountTableProperty.NAME_OF_COLUMN_OF_ID.getValue()),
        /*
            SELECT users.id, email, encrypted_password, name, surname, patronymic, phone_number, bank_account_id
            FROM clients LEFT OUTER JOIN users
                ON users.id = clients.id
                LEFT OUTER JOIN bank_accounts
                    ON clients.bank_account_id = bank_accounts.id
            WHERE clients.id = ?
         */
        PREPARED_STATEMENT_TO_SELECT_CLIENT_BY_ID("SELECT " + UserTableProperty.NAME_OF_TABLE.getValue() + "."
                + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue() + ", "
                + NAME_OF_COLUMN_OF_NAME.getValue() + ", " + NAME_OF_COLUMN_OF_SURNAME.getValue() + ", "
                + NAME_OF_COLUMN_OF_PATRONYMIC.getValue() + ", " + NAME_OF_COLUMN_OF_PHONE_NUMBER.getValue() + ", "
                + NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue() + " FROM " + ClientTableProperty.NAME_OF_TABLE.getValue()
                + " LEFT OUTER JOIN " + UserTableProperty.NAME_OF_TABLE.getValue() + " ON "
                + ClientTableProperty.NAME_OF_TABLE.getValue() + "." + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue()
                + " = " + UserTableProperty.NAME_OF_TABLE.getValue() + "." + UserTableProperty.NAME_OF_COLUMN_OF_ID.getValue()
                + " LEFT OUTER JOIN " + BankAccountTableProperty.NAME_OF_TABLE.getValue() + " ON "
                + ClientTableProperty.NAME_OF_TABLE.getValue() + "." + NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue()
                + " = " + BankAccountTableProperty.NAME_OF_TABLE.getValue() + "."
                + BankAccountTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + " WHERE "
                + ClientTableProperty.NAME_OF_TABLE.getValue() + "." + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue()
                + " = ?"),
        PREPARED_STATEMENT_TO_UPDATE_CLIENT("UPDATE " + ClientTableProperty.NAME_OF_TABLE.getValue() + " SET "
                + NAME_OF_COLUMN_OF_NAME.getValue() + " = ?, " + NAME_OF_COLUMN_OF_SURNAME.getValue() + " = ?, "
                + NAME_OF_COLUMN_OF_PATRONYMIC.getValue() + " = ?, " + NAME_OF_COLUMN_OF_PATRONYMIC.getValue() + " = ?, "
                + NAME_OF_COLUMN_OF_PHONE_NUMBER.getValue() + " = ?, " + NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue() + " = ?"
                + " WHERE " + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_DELETE_BY_ID("DELETE FROM " + ClientTableProperty.NAME_OF_TABLE.getValue() + " WHERE "
                + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_DEFINE_EXISTING_BY_ID("SELECT 1 FROM " + ClientTableProperty.NAME_OF_TABLE.getValue()
                + " WHERE " + ClientTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + " = ?");

        private final String sqlQuery;

        private ClientSqlOperation(final String sqlQuery)
        {
            this.sqlQuery = sqlQuery;
        }

        public final String getSqlQuery()
        {
            return this.sqlQuery;
        }

        static final int PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_INSERT_USER = 1;
        static final int PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_INSERT_USER = 2;

        static final int PARAMETER_INDEX_OF_NAME_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT = 1;
        static final int PARAMETER_INDEX_OF_SURNAME_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT = 2;
        static final int PARAMETER_INDEX_OF_PATRONYMIC_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT = 3;
        static final int PARAMETER_INDEX_OF_PHONE_NUMBER_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT = 4;
        static final int PARAMETER_INDEX_OF_BANK_ACCOUNT_ID_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT = 5;
        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_INSERT_CLIENT = 6;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_SELECT_BY_ID = 1;

        static final int PARAMETER_INDEX_OF_NAME_IN_PREPARED_STATEMENT_TO_UPDATE = 1;
        static final int PARAMETER_INDEX_OF_SURNAME_IN_PREPARED_STATEMENT_TO_UPDATE = 2;
        static final int PARAMETER_INDEX_OF_PATRONYMIC_IN_PREPARED_STATEMENT_TO_UPDATE = 3;
        static final int PARAMETER_INDEX_OF_PHONE_NUMBER_IN_PREPARED_STATEMENT_TO_UPDATE = 4;
        static final int PARAMETER_INDEX_OF_BANK_ACCOUNT_ID_IN_PREPARED_STATEMENT_TO_UPDATE = 5;
        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_UPDATE = 6;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DELETE = 1;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DEFINE_EXISTING = 1;
    }
}
