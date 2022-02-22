package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.user;

import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.entity.BankAccount;
import by.itacademy.zuevvlad.cardpaymentproject.entity.User;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty.*;

public final class ResultSetMapperToUsers implements ResultSetMapperToCollection<User, Set<User>>
{
    private final ResultSetMapperToCollection.ResultSetRowMapperToEntity<User> resultSetRowMapperToUser;

    public static ResultSetMapperToCollection<User, Set<User>> createResultSetMapperToUsers()
    {
        if(ResultSetMapperToUsers.resultSetMapperToUsers == null)
        {
            synchronized(ResultSetMapperToUsers.class)
            {
                if(ResultSetMapperToUsers.resultSetMapperToUsers == null)
                {
                    ResultSetMapperToUsers.resultSetMapperToUsers = new ResultSetMapperToUsers();
                }
            }
        }
        return ResultSetMapperToUsers.resultSetMapperToUsers;
    }

    private static ResultSetMapperToUsers resultSetMapperToUsers = null;

    private ResultSetMapperToUsers()
    {
        super();

        this.resultSetRowMapperToUser = new ResultSetRowMapperToUser();
    }

    @Override
    public final ResultSetMapperToCollection.ResultSetRowMapperToEntity<User> getResultSetRowMapperToEntity()
    {
        return this.resultSetRowMapperToUser;
    }

    @Override
    public final Set<User> map(final ResultSet mappedResultSet)
            throws ResultSetMappingToCollectionException
    {
        try
        {
            final Set<User> mappedUsers= new LinkedHashSet<User>();   //TODO: если будет можно заменить на HashSet
            User currentMappedUser;
            while(mappedResultSet.next())
            {
                currentMappedUser = this.resultSetRowMapperToUser.mapCurrentRow(mappedResultSet);
                mappedUsers.add(currentMappedUser);
            }
            return mappedUsers;
        }
        catch(final SQLException | ResultSetRowMappingToEntityException cause)
        {
            throw new ResultSetMappingToCollectionException(cause);
        }
    }

    private static final class ResultSetRowMapperToUser
            implements ResultSetMapperToCollection.ResultSetRowMapperToEntity<User>
    {
        private final Cryptographer<String, String> cryptographer;

        public ResultSetRowMapperToUser()
        {
            super();

            this.cryptographer = StringToStringCryptographer.createCryptographer();
        }

        @Override
        public final User mapCurrentRow(final ResultSet resultSetOfMappedRow)
            throws ResultSetRowMappingToEntityException
        {
            try
            {
                final long id = resultSetOfMappedRow.getLong(NAME_OF_COLUMN_OF_ID.getValue());
                final String email = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_EMAIL.getValue());

                final String encryptedPassword = resultSetOfMappedRow.getString(
                        NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue());
                final String password = this.cryptographer.decrypt(encryptedPassword);

                return new User(id, email, password);
            }
            catch(final SQLException cause)
            {
                throw new ResultSetRowMappingToEntityException(cause);
            }
        }
    }
}
