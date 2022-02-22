package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.administrator;

import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.ResultSetMapperToCollection;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty;
import by.itacademy.zuevvlad.cardpaymentproject.entity.Administrator;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.UserTableProperty.*;
import static by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty.AdministratorTableProperty.*;

public final class ResultSetMapperToAdministrators
        implements ResultSetMapperToCollection<Administrator, Set<Administrator>>
{
    private final ResultSetMapperToCollection.ResultSetRowMapperToEntity<Administrator> resultSetRowMapperToAdministrator;

    public static ResultSetMapperToCollection<Administrator, Set<Administrator>> createResultSetMapperToAdministrators()
    {
        if(ResultSetMapperToAdministrators.resultSetMapperToAdministrators == null)
        {
            synchronized(ResultSetMapperToAdministrators.class)
            {
                if(ResultSetMapperToAdministrators.resultSetMapperToAdministrators == null)
                {
                    ResultSetMapperToAdministrators.resultSetMapperToAdministrators
                            = new ResultSetMapperToAdministrators();
                }
            }
        }
        return ResultSetMapperToAdministrators.resultSetMapperToAdministrators;
    }

    private static ResultSetMapperToAdministrators resultSetMapperToAdministrators = null;

    private ResultSetMapperToAdministrators()
    {
        super();

        this.resultSetRowMapperToAdministrator = new ResultSetRowMapperToAdministrator();
    }

    @Override
    public final ResultSetMapperToCollection.ResultSetRowMapperToEntity<Administrator> getResultSetRowMapperToEntity()
    {
        return this.resultSetRowMapperToAdministrator;
    }

    @Override
    public final Set<Administrator> map(final ResultSet mappedResultSet)
            throws ResultSetMappingToCollectionException
    {
        try
        {
            final Set<Administrator> mappedAdministrators = new LinkedHashSet<Administrator>();  //TODO: если будет можно заменить на HashSet
            Administrator currentMappedAdministrator;
            while(mappedResultSet.next())
            {
                currentMappedAdministrator = this.resultSetRowMapperToAdministrator.mapCurrentRow(mappedResultSet);
                mappedAdministrators.add(currentMappedAdministrator);
            }
            return mappedAdministrators;
        }
        catch(final SQLException | ResultSetRowMappingToEntityException cause)
        {
            throw new ResultSetMappingToCollectionException(cause);
        }
    }

    private static final class ResultSetRowMapperToAdministrator
            implements ResultSetMapperToCollection.ResultSetRowMapperToEntity<Administrator>
    {
        private final Cryptographer<String, String> cryptographer;

        public ResultSetRowMapperToAdministrator()
        {
            super();

            this.cryptographer = StringToStringCryptographer.createCryptographer();
        }

        @Override
        public final Administrator mapCurrentRow(final ResultSet resultSetOfMappedRow)
                throws ResultSetRowMappingToEntityException
        {
            try
            {
                final long id = resultSetOfMappedRow.getLong(UserTableProperty.NAME_OF_COLUMN_OF_ID.getValue());
                final String email = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_EMAIL.getValue());

                final String encryptedPassword = resultSetOfMappedRow.getString(
                        NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD.getValue());
                final String password = this.cryptographer.decrypt(encryptedPassword);

                final String nameOfLevel = resultSetOfMappedRow.getString(NAME_OF_COLUMN_OF_LEVEL.getValue());
                final Administrator.Level level = Administrator.Level.valueOf(nameOfLevel);

                return new Administrator(id, email, password, level);
            }
            catch(final SQLException cause)
            {
                throw new ResultSetRowMappingToEntityException(cause);
            }
        }
    }
}
