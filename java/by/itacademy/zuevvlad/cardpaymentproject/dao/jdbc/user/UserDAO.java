package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.user;

import by.itacademy.zuevvlad.cardpaymentproject.dao.DAO;
import by.itacademy.zuevvlad.cardpaymentproject.dao.exception.*;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.DataBaseConnectionPool;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.exception.DataBaseConnectionPoolAccessConnectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.FounderGeneratedIdByDataBase;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.exception.FindingGeneratedIdByDataBaseException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.user.ResultSetMapperToUsers;
import by.itacademy.zuevvlad.cardpaymentproject.entity.User;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.sql.*;
import java.util.Optional;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty.*;

public final class UserDAO implements DAO<User>
{
    private final DataBaseConnectionPool dataBaseConnectionPool;
    private final ResultSetMapperToCollection<User, Set<User>> resultSetMapperToUsers;
    private final ResultSetMapperToCollection.ResultSetRowMapperToEntity<User> resultSetRowMapperToUser;
    private final Cryptographer<String, String> cryptographer;
    private final FounderGeneratedIdByDataBase founderGeneratedIdByDataBase;

    public UserDAO()
    {
        super();

        this.dataBaseConnectionPool = DataBaseConnectionPool.createDataBaseConnectionPool();
        this.resultSetMapperToUsers = ResultSetMapperToUsers.createResultSetMapperToUsers();
        this.resultSetRowMapperToUser = this.resultSetMapperToUsers.getResultSetRowMapperToEntity();
        this.cryptographer = StringToStringCryptographer.createCryptographer();
        this.founderGeneratedIdByDataBase = FounderGeneratedIdByDataBase.createFounderGeneratedIdByDataBase();
    }

    @Override
    public final void addEntity(final User addedUser)
            throws AddingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    UserSqlOperation.PREPARED_STATEMENT_TO_INSERT_USER.getSqlQuery()))
            {
                preparedStatement.setString(UserSqlOperation.PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_INSERT,
                        addedUser.getEmail());

                final String encryptedPassword = this.cryptographer.encrypt(addedUser.getPassword());
                preparedStatement.setString(
                        UserSqlOperation.PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_INSERT,
                        encryptedPassword);

                preparedStatement.executeUpdate();

                final long generatedId = this.founderGeneratedIdByDataBase.findGeneratedIdInLastInserting(
                        preparedStatement, NAME_OF_COLUMN_OF_ID.getValue());
                addedUser.setId(generatedId);
            }
            finally
            {
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException
                | FindingGeneratedIdByDataBaseException cause)
        {
            throw new AddingEntityException(cause);
        }
    }

    @Override
    public final Iterable<User> offloadEntities()
            throws OffloadingEntitiesException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final Statement statement = connectionToDataBase.createStatement())
            {
                final ResultSet resultSet = statement.executeQuery(UserSqlOperation.OFFLOAD_ALL_USERS.getSqlQuery());
                return this.resultSetMapperToUsers.map(resultSet);
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
    public final Optional<User> findEntityById(final long idOfFoundUser)
            throws FindingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    UserSqlOperation.PREPARED_STATEMENT_TO_SELECT_USER_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(UserSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_SELECT_BY_ID,
                        idOfFoundUser);

                final ResultSet resultSet = preparedStatement.executeQuery();
                return resultSet.next() ? Optional.of(this.resultSetRowMapperToUser.mapCurrentRow(resultSet))
                        : Optional.empty();
            }
            finally
            {
                this.dataBaseConnectionPool.returnConnectionToPool(connectionToDataBase);
            }
        }
        catch(final DataBaseConnectionPoolAccessConnectionException | SQLException
                | ResultSetRowMappingToEntityException cause)
        {
            throw new FindingEntityException(cause);
        }
    }

    @Override
    public final void updateEntity(final User updatedUser)
            throws UpdatingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    UserSqlOperation.PREPARED_STATEMENT_TO_UPDATE_USER.getSqlQuery()))
            {
                preparedStatement.setString(UserSqlOperation.PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedUser.getEmail());

                final String encryptedPassword = this.cryptographer.encrypt(updatedUser.getPassword());
                preparedStatement.setString(
                        UserSqlOperation.PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_UPDATE,
                        encryptedPassword);

                preparedStatement.setLong(UserSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedUser.getId());

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
    public final void deleteEntityById(final long idOfDeletedUser)
            throws DeletingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    UserSqlOperation.PREPARED_STATEMENT_TO_DELETE_USER_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(UserSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DELETE,
                        idOfDeletedUser);

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
    public final boolean isEntityWithGivenIdExisting(final long idOfResearchUser)
            throws DefiningExistingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    UserSqlOperation.PREPARED_STATEMENT_TO_DEFINE_EXISTING_USER_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(
                        UserSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DEFINE_EXISTING,
                        idOfResearchUser);

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

    private static enum UserSqlOperation
    {
        PREPARED_STATEMENT_TO_INSERT_USER("INSERT INTO " + NAME_OF_TABLE.getValue() + " ("
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue()
                + ") VALUES (?, ?)"),
        OFFLOAD_ALL_USERS("SELECT " + NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue()
                + " FROM " + NAME_OF_TABLE.getValue()),
        PREPARED_STATEMENT_TO_SELECT_USER_BY_ID("SELECT " + NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue()
                + " FROM " + NAME_OF_TABLE.getValue() + " WHERE " + NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_UPDATE_USER("UPDATE " + NAME_OF_TABLE.getValue() + " SET "
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + " = ?, "
                + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue() + " = ? WHERE "
                + NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_DELETE_USER_BY_ID("DELETE FROM " + NAME_OF_TABLE.getValue() + " WHERE "
                + NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_DEFINE_EXISTING_USER_BY_ID("SELECT 1 FROM " + NAME_OF_TABLE.getValue() + " WHERE "
                + NAME_OF_COLUMN_OF_ID.getValue() + " = ?");

        private final String sqlQuery;

        private UserSqlOperation(final String sqlQuery)
        {
            this.sqlQuery = sqlQuery;
        }

        public final String getSqlQuery()
        {
            return this.sqlQuery;
        }

        static final int PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_INSERT = 1;
        static final int PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_INSERT = 2;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_SELECT_BY_ID = 1;

        static final int PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_UPDATE = 1;
        static final int PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_UPDATE = 2;
        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_UPDATE = 3;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DELETE = 1;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DEFINE_EXISTING = 1;
    }
}
