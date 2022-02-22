package by.itacademy.zuevvlad.cardpaymentproject.dao.exception;

public final class OffloadingEntitiesException extends DAOException
{
    public OffloadingEntitiesException()
    {
        super();
    }

    public OffloadingEntitiesException(final String description)
    {
        super(description);
    }

    public OffloadingEntitiesException(final Exception cause)
    {
        super(cause);
    }

    public OffloadingEntitiesException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
