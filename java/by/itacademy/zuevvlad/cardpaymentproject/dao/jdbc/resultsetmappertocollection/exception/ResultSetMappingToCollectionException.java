package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception;

public final class ResultSetMappingToCollectionException extends Exception
{
    public ResultSetMappingToCollectionException()
    {
        super();
    }

    public ResultSetMappingToCollectionException(final String description)
    {
        super(description);
    }

    public ResultSetMappingToCollectionException(final Exception cause)
    {
        super(cause);
    }

    public ResultSetMappingToCollectionException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
