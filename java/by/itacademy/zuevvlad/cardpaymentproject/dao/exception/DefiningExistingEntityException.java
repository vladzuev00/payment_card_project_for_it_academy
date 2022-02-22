package by.itacademy.zuevvlad.cardpaymentproject.dao.exception;

public final class DefiningExistingEntityException extends DAOException
{
    public DefiningExistingEntityException()
    {
        super();
    }

    public DefiningExistingEntityException(final String description)
    {
        super(description);
    }

    public DefiningExistingEntityException(final Exception cause)
    {
        super(cause);
    }

    public DefiningExistingEntityException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
