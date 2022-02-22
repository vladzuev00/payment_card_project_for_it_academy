package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception;

public final class ResultSetRowMappingToEntityException extends Exception
{
    public ResultSetRowMappingToEntityException()
    {
        super();
    }

    public ResultSetRowMappingToEntityException(final String description)
    {
        super(description);
    }

    public ResultSetRowMappingToEntityException(final Exception cause)
    {
        super(cause);
    }

    public ResultSetRowMappingToEntityException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
