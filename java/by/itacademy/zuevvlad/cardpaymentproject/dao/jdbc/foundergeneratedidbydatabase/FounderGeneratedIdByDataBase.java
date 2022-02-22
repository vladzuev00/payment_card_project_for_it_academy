package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase;

import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.foundergeneratedidbydatabase.exception.FindingGeneratedIdByDataBaseException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class FounderGeneratedIdByDataBase
{
    public static FounderGeneratedIdByDataBase createFounderGeneratedIdByDataBase()
    {
        if(FounderGeneratedIdByDataBase.founderGeneratedIdByDataBase == null)
        {
            synchronized(FounderGeneratedIdByDataBase.class)
            {
                if(FounderGeneratedIdByDataBase.founderGeneratedIdByDataBase == null)
                {
                    FounderGeneratedIdByDataBase.founderGeneratedIdByDataBase = new FounderGeneratedIdByDataBase();
                }
            }
        }
        return FounderGeneratedIdByDataBase.founderGeneratedIdByDataBase;
    }

    private static FounderGeneratedIdByDataBase founderGeneratedIdByDataBase = null;

    private FounderGeneratedIdByDataBase()
    {
        super();
    }

    public final long findGeneratedIdInLastInserting(final Statement insertingStatement,
                                                     final String nameOfColumnOfId)
            throws FindingGeneratedIdByDataBaseException
    {
        try(final ResultSet resultSetOfGeneratedKeys = insertingStatement.getGeneratedKeys())
        {
            resultSetOfGeneratedKeys.next();
            return resultSetOfGeneratedKeys.getLong(nameOfColumnOfId);
        }
        catch(final SQLException cause)
        {
            throw new FindingGeneratedIdByDataBaseException(cause);
        }
    }
}
