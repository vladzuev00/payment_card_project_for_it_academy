package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.client;

import by.itacademy.zuevvlad.cardpaymentproject.dao.DAO;
import by.itacademy.zuevvlad.cardpaymentproject.dao.exception.FindingEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.bankaccount.BankAccountDAO;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.entity.BankAccount;
import by.itacademy.zuevvlad.cardpaymentproject.entity.Client;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty.*;
import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.ClientTableProperty.*;

public final class ResultSetMapperToClients implements ResultSetMapperToCollection<Client, Set<Client>>
{
    private final ResultSetMapperToCollection.ResultSetRowMapperToEntity<Client> resultSetRowMapperToClient;

    public static ResultSetMapperToCollection<Client, Set<Client>> createResultSetMapperToClients()
    {
        if(ResultSetMapperToClients.resultSetMapperToClients == null)
        {
            synchronized(ResultSetMapperToClients.class)
            {
                if(ResultSetMapperToClients.resultSetMapperToClients == null)
                {
                    ResultSetMapperToClients.resultSetMapperToClients = new ResultSetMapperToClients();
                }
            }
        }
        return ResultSetMapperToClients.resultSetMapperToClients;
    }

    private static ResultSetMapperToClients resultSetMapperToClients = null;

    private ResultSetMapperToClients()
    {
        super();

        this.resultSetRowMapperToClient = new ResultSetRowMapperToClient();
    }

    @Override
    public final ResultSetMapperToCollection.ResultSetRowMapperToEntity<Client> getResultSetRowMapperToEntity()
    {
        return this.resultSetRowMapperToClient;
    }

    @Override
    public final Set<Client> map(final ResultSet mappedResultSet)
            throws ResultSetMappingToCollectionException
    {
        try
        {
            final Set<Client> mappedClients = new LinkedHashSet<Client>();       //TODO: если будет можно заменить на HashSet
            Client currentMappedClient;
            while(mappedResultSet.next())
            {
                currentMappedClient = this.resultSetRowMapperToClient.mapCurrentRow(mappedResultSet);
                mappedClients.add(currentMappedClient);
            }
            return mappedClients;
        }
        catch(final SQLException | ResultSetRowMappingToEntityException cause)
        {
            throw new ResultSetMappingToCollectionException(cause);
        }
    }

    private static final class ResultSetRowMapperToClient
            implements ResultSetMapperToCollection.ResultSetRowMapperToEntity<Client>
    {
        private final Cryptographer<String, String> cryptographer;
        private final DAO<BankAccount> bankAccountDAO;

        public ResultSetRowMapperToClient()
        {
            super();

            this.cryptographer = StringToStringCryptographer.createCryptographer();
            this.bankAccountDAO = new BankAccountDAO();
        }

        @Override
        public final Client mapCurrentRow(final ResultSet resultSetOfMappedRow)
                throws ResultSetRowMappingToEntityException
        {
            try
            {
                final long id = resultSetOfMappedRow.getLong(UserTableProperty.NAME_OF_COLUMN_OF_ID.getValue());
                final String email = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_EMAIL.getValue());

                final String encryptedPassword = resultSetOfMappedRow.getString(
                        NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue());
                final String password = this.cryptographer.decrypt(encryptedPassword);

                final String name = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_NAME.getValue());
                final String surname = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_SURNAME.getValue());
                final String patronymic = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_PATRONYMIC.getValue());
                final String phoneNumber = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_PHONE_NUMBER.getValue());

                final long bankAccountId = resultSetOfMappedRow.getLong(NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID.getValue());
                final Optional<BankAccount> optionalOfBankAccount = this.bankAccountDAO.findEntityById(bankAccountId);
                if(!optionalOfBankAccount.isPresent())
                {
                    throw new ResultSetRowMappingToEntityException("Bank account with id = " + bankAccountId
                            + " doesn't exist.");
                }
                final BankAccount bankAccount = optionalOfBankAccount.get();

                return new Client(id, email, password, name, surname, patronymic, phoneNumber, bankAccount);
            }
            catch(final SQLException | FindingEntityException cause)
            {
                throw new ResultSetRowMappingToEntityException(cause);
            }
        }
    }
}
