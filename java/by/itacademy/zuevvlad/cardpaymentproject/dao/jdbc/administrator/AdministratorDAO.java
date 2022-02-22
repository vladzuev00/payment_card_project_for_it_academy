package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.administrator;

import by.itacademy.zuevvlad.cardpaymentproject.dao.DAO;
import by.itacademy.zuevvlad.cardpaymentproject.dao.exception.AddingEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.DataBaseConnectionPool;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.exception.DataBaseConnectionPoolAccessConnectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.FounderGeneratedIdByDataBase;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.exception.FindingGeneratedIdByDataBaseException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.AdministratorTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.entity.Administrator;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty.*;
import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.AdministratorTableProperty.*;

public final class AdministratorDAO implements DAO<Administrator>
{
    private final DataBaseConnectionPool dataBaseConnectionPool;
    private final Cryptographer<String, String> cryptographer;
    private final FounderGeneratedIdByDataBase founderGeneratedIdByDataBase;

    public AdministratorDAO()
    {
        super();

        this.dataBaseConnectionPool = DataBaseConnectionPool.createDataBaseConnectionPool();
        this.cryptographer = StringToStringCryptographer.createCryptographer();
        this.founderGeneratedIdByDataBase = FounderGeneratedIdByDataBase.createFounderGeneratedIdByDataBase();
    }

    @Override
    public final void addEntity(final Administrator addedAdministrator)
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
                    this.addInTableOfUsers(addedAdministrator, connectionToDataBase);
                    this.addInTableOfAdministrators(addedAdministrator, connectionToDataBase);
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

    private void addInTableOfUsers(final Administrator addedAdministrator, final Connection connectionToDataBase)
            throws AddingEntityException
    {
        try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                AdministratorSqlOperation.PREPARED_STATEMENT_TO_INSERT_USER.getSqlQuery()))
        {
            preparedStatement.setString(
                    AdministratorSqlOperation.PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_INSERT_USER,
                    addedAdministrator.getEmail());

            final String encryptedPassword = this.cryptographer.encrypt(addedAdministrator.getPassword());
            preparedStatement.setString(
                    AdministratorSqlOperation.PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_INSERT_USER,
                    encryptedPassword);

            preparedStatement.executeUpdate();

            final FounderGeneratedIdByDataBase founderGeneratedIdByDataBase
                    = FounderGeneratedIdByDataBase.createFounderGeneratedIdByDataBase();
            final long generatedId = founderGeneratedIdByDataBase.findGeneratedIdInLastInserting(preparedStatement,
                    UserTableProperty.NAME_OF_COLUMN_OF_ID.getValue());
            addedAdministrator.setId(generatedId);
        }
        catch(final SQLException | FindingGeneratedIdByDataBaseException cause)
        {
            throw new AddingEntityException(cause);
        }
    }

    private void addInTableOfAdministrators(final Administrator addedAdministrator,
                                            final Connection connectionToDataBase)
            throws AddingEntityException
    {
        try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                AdministratorSqlOperation.PREPARED_STATEMENT_TO_INSERT_ADMINISTRATOR.getSqlQuery()))
        {
            preparedStatement.setLong(AdministratorSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_INSERT,
                    addedAdministrator.getId());
            preparedStatement.setString(
                    AdministratorSqlOperation.PARAMETER_INDEX_OF_LEVEL_IN_PREPARED_STATEMENT_TO_INSERT,
                    addedAdministrator.getLevel().name());
            preparedStatement.executeUpdate();
        }
        catch(final SQLException cause)
        {
            throw new AddingEntityException(cause);
        }
    }

    @Override
    public final Iterable<Administrator> offloadEntities()

    private static enum AdministratorSqlOperation
    {
        PREPARED_STATEMENT_TO_INSERT_USER("INSERT INTO " + UserTableProperty.NAME_OF_TABLE.getValue() + " ("
                + NAME_OF_COLUMN_OF_EMAIL.getValue() + ", " + NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue()
                + ") VALUES (?, ?)"),
        PREPARED_STATEMENT_TO_INSERT_ADMINISTRATOR("INSERT INTO " + AdministratorTableProperty.NAME_OF_TABLE.getValue()
                + " (" + AdministratorTableProperty.NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_LEVEL.getValue() + ") VALUES (?, ?)");

        private final String sqlQuery;

        private AdministratorSqlOperation(final String sqlQuery)
        {
            this.sqlQuery = sqlQuery;
        }

        public final String getSqlQuery()
        {
            return this.sqlQuery;
        }

        static final int PARAMETER_INDEX_OF_EMAIL_IN_PREPARED_STATEMENT_TO_INSERT_USER = 1;
        static final int PARAMETER_INDEX_OF_ENCRYPTED_PASSWORD_IN_PREPARED_STATEMENT_TO_INSERT_USER = 2;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_INSERT = 1;
        static final int PARAMETER_INDEX_OF_LEVEL_IN_PREPARED_STATEMENT_TO_INSERT = 2;
    }
}
