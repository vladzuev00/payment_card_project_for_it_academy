package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.bankaccount;

import by.itacademy.zuevvlad.cardpaymentproject.dao.DAO;
import by.itacademy.zuevvlad.cardpaymentproject.dao.exception.*;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.DataBaseConnectionPool;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.databaseconnectionpool.exception.DataBaseConnectionPoolAccessConnectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.FounderGeneratedIdByDataBase;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.exception.FindingGeneratedIdByDataBaseException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.bankaccount.ResultSetMapperToBankAccounts;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.entity.BankAccount;

import java.sql.*;
import java.util.Optional;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.BankAccountTableProperty.*;

public final class BankAccountDAO implements DAO<BankAccount>
{
    private final DataBaseConnectionPool dataBaseConnectionPool;
    private final ResultSetMapperToCollection<BankAccount, Set<BankAccount>> resultSetMapperToBankAccounts;
    private final ResultSetMapperToCollection.ResultSetRowMapperToEntity<BankAccount> resultSetRowMapperToBankAccount;
    private final FounderGeneratedIdByDataBase founderGeneratedIdByDataBase;

    public BankAccountDAO()
    {
        super();

        this.dataBaseConnectionPool = DataBaseConnectionPool.createDataBaseConnectionPool();
        this.resultSetMapperToBankAccounts = ResultSetMapperToBankAccounts.createResultSetMapperToBankAccounts();
        this.resultSetRowMapperToBankAccount = this.resultSetMapperToBankAccounts.getResultSetRowMapperToEntity();
        this.founderGeneratedIdByDataBase = FounderGeneratedIdByDataBase.createFounderGeneratedIdByDataBase();
    }

    @Override
    public final void addEntity(final BankAccount addedBankAccount)
            throws AddingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    BankAccountSqlOperation.PREPARED_STATEMENT_TO_INSERT_BANK_ACCOUNT.getSqlQuery()))
            {
                preparedStatement.setBigDecimal(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_MONEY_IN_PREPARED_STATEMENT_TO_INSERT,
                        addedBankAccount.getMoney());
                preparedStatement.setBoolean(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_BLOCKED_IN_PREPARED_STATEMENT_TO_INSERT,
                        addedBankAccount.isBlocked());

                preparedStatement.executeUpdate();

                final long generatedId = this.founderGeneratedIdByDataBase.findGeneratedIdInLastInserting(
                        preparedStatement, NAME_OF_COLUMN_OF_ID.getValue());
                addedBankAccount.setId(generatedId);
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
    public final Iterable<BankAccount> offloadEntities()
            throws OffloadingEntitiesException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final Statement statement = connectionToDataBase.createStatement())
            {
                final ResultSet resultSet = statement.executeQuery(
                        BankAccountSqlOperation.OFFLOAD_ALL_BANK_ACCOUNTS.getSqlQuery());
                return this.resultSetMapperToBankAccounts.map(resultSet);
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
    public final Optional<BankAccount> findEntityById(final long idOfFoundBankAccount)
            throws FindingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    BankAccountSqlOperation.PREPARED_STATEMENT_TO_SELECT_BANK_ACCOUNT_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_SELECT_BY_ID,
                        idOfFoundBankAccount);

                final ResultSet resultSet = preparedStatement.executeQuery();

                return resultSet.next() ? Optional.of(this.resultSetRowMapperToBankAccount.mapCurrentRow(resultSet))
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
    public final void updateEntity(final BankAccount updatedBankAccount)
            throws UpdatingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    BankAccountSqlOperation.PREPARED_STATEMENT_TO_UPDATE_BANK_ACCOUNT.getSqlQuery()))
            {
                preparedStatement.setBigDecimal(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_MONEY_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedBankAccount.getMoney());
                preparedStatement.setBoolean(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_BLOCKED_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedBankAccount.isBlocked());
                preparedStatement.setLong(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_UPDATE,
                        updatedBankAccount.getId());

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
    public final void deleteEntityById(final long idOfDeletedBankAccount)
            throws DeletingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    BankAccountSqlOperation.PREPARED_STATEMENT_TO_DELETE_BANK_ACCOUNT_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DELETE_BY_ID,
                        idOfDeletedBankAccount);

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
    public final boolean isEntityWithGivenIdExisting(final long idOfResearchBankAccount)
            throws DefiningExistingEntityException
    {
        try
        {
            final Connection connectionToDataBase = this.dataBaseConnectionPool.findAvailableConnection();
            try(final PreparedStatement preparedStatement = connectionToDataBase.prepareStatement(
                    BankAccountSqlOperation.PREPARED_STATEMENT_TO_DEFINE_EXISTING_BANK_ACCOUNT_BY_ID.getSqlQuery()))
            {
                preparedStatement.setLong(
                        BankAccountSqlOperation.PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DEFINE_EXISTING,
                        idOfResearchBankAccount);
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

    private static enum BankAccountSqlOperation
    {
        PREPARED_STATEMENT_TO_INSERT_BANK_ACCOUNT("INSERT INTO " + NAME_OF_TABLE.getValue()
                + " (" + NAME_OF_COLUMN_OF_MONEY.getValue() + ", " + NAME_OF_COLUMN_OF_BLOCKED.getValue() + ")"
                + "VALUES(?, ?)"),
        OFFLOAD_ALL_BANK_ACCOUNTS("SELECT " + NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_MONEY.getValue() + ", " + NAME_OF_COLUMN_OF_BLOCKED.getValue()
                + " FROM " + NAME_OF_TABLE.getValue()),
        PREPARED_STATEMENT_TO_SELECT_BANK_ACCOUNT_BY_ID("SELECT " + NAME_OF_COLUMN_OF_ID.getValue() + ", "
                + NAME_OF_COLUMN_OF_MONEY.getValue() + ", " + NAME_OF_COLUMN_OF_BLOCKED.getValue()
                + " FROM " + NAME_OF_TABLE.getValue()
                + " WHERE " + NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_UPDATE_BANK_ACCOUNT("UPDATE " + NAME_OF_TABLE.getValue() + " SET "
                + NAME_OF_COLUMN_OF_MONEY.getValue() + " = ?, " + NAME_OF_COLUMN_OF_BLOCKED.getValue() + " = ? "
                + "WHERE " + NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_DELETE_BANK_ACCOUNT_BY_ID("DELETE FROM " + NAME_OF_TABLE.getValue()
                + " WHERE " + NAME_OF_COLUMN_OF_ID.getValue() + " = ?"),
        PREPARED_STATEMENT_TO_DEFINE_EXISTING_BANK_ACCOUNT_BY_ID("SELECT 1 FROM " + NAME_OF_TABLE.getValue()
                + " WHERE " + NAME_OF_COLUMN_OF_ID.getValue() + " = ?");

        private final String sqlQuery;

        private BankAccountSqlOperation(final String sqlQuery)
        {
            this.sqlQuery = sqlQuery;
        }

        public final String getSqlQuery()
        {
            return this.sqlQuery;
        }

        static final int PARAMETER_INDEX_OF_MONEY_IN_PREPARED_STATEMENT_TO_INSERT = 1;
        static final int PARAMETER_INDEX_OF_BLOCKED_IN_PREPARED_STATEMENT_TO_INSERT = 2;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_SELECT_BY_ID = 1;

        static final int PARAMETER_INDEX_OF_MONEY_IN_PREPARED_STATEMENT_TO_UPDATE = 1;
        static final int PARAMETER_INDEX_OF_BLOCKED_IN_PREPARED_STATEMENT_TO_UPDATE = 2;
        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_UPDATE = 3;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DELETE_BY_ID = 1;

        static final int PARAMETER_INDEX_OF_ID_IN_PREPARED_STATEMENT_TO_DEFINE_EXISTING = 1;
    }
}
