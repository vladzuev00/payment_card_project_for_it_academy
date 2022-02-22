package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.bankaccount;

import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.entity.BankAccount;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.BankAccountTableProperty.*;

public final class ResultSetMapperToBankAccounts implements ResultSetMapperToCollection<BankAccount, Set<BankAccount>>
{
    private final ResultSetRowMapperToEntity<BankAccount> resultSetRowMapperToBankAccount;

    public static ResultSetMapperToCollection<BankAccount, Set<BankAccount>> createResultSetMapperToBankAccounts()
    {
        if(ResultSetMapperToBankAccounts.resultSetMapperToBankAccounts == null)
        {
            synchronized(ResultSetMapperToBankAccounts.class)
            {
                if(ResultSetMapperToBankAccounts.resultSetMapperToBankAccounts == null)
                {
                    ResultSetMapperToBankAccounts.resultSetMapperToBankAccounts = new ResultSetMapperToBankAccounts();
                }
            }
        }
        return ResultSetMapperToBankAccounts.resultSetMapperToBankAccounts;
    }

    private static ResultSetMapperToBankAccounts resultSetMapperToBankAccounts = null;

    private ResultSetMapperToBankAccounts()
    {
        super();

        this.resultSetRowMapperToBankAccount = new ResultSetRowMapperToBankAccount();
    }

    @Override
    public final ResultSetMapperToCollection.ResultSetRowMapperToEntity<BankAccount> getResultSetRowMapperToEntity()
    {
        return this.resultSetRowMapperToBankAccount;
    }

    @Override
    public final Set<BankAccount> map(final ResultSet mappedResultSet)
            throws ResultSetMappingToCollectionException
    {
        try
        {
            final Set<BankAccount> mappedBankAccounts = new LinkedHashSet<BankAccount>();   //TODO: если будет можно заменить на HashSet
            BankAccount currentMappedBankAccount;
            while(mappedResultSet.next())
            {
                currentMappedBankAccount = this.resultSetRowMapperToBankAccount.mapCurrentRow(mappedResultSet);
                mappedBankAccounts.add(currentMappedBankAccount);
            }
            return mappedBankAccounts;
        }
        catch(final SQLException | ResultSetRowMappingToEntityException cause)
        {
            throw new ResultSetMappingToCollectionException(cause);
        }
    }

    private static final class ResultSetRowMapperToBankAccount
            implements ResultSetMapperToCollection.ResultSetRowMapperToEntity<BankAccount>
    {
        public ResultSetRowMapperToBankAccount()
        {
            super();
        }

        @Override
        public final BankAccount mapCurrentRow(final ResultSet resultSetOfMappedRow)
                throws ResultSetRowMappingToEntityException
        {
            try
            {
                final long id = resultSetOfMappedRow.getLong(NAME_OF_COLUMN_OF_ID.getValue());
                final BigDecimal money = resultSetOfMappedRow.getBigDecimal(NAME_OF_COLUMN_OF_MONEY.getValue());
                final boolean blocked = resultSetOfMappedRow.getBoolean(NAME_OF_COLUMN_OF_BLOCKED.getValue());
                return new BankAccount(id, money, blocked);
            }
            catch(final SQLException cause)
            {
                throw new ResultSetRowMappingToEntityException(cause);
            }
        }
    }
}
