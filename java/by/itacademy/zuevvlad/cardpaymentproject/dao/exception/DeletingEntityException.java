package by.itacademy.zuevvlad.cardpaymentproject.dao.exception;

public final class DeletingEntityException extends DAOException
{
    public DeletingEntityException()
    {
        super();
    }

    public DeletingEntityException(final String description)
    {
        super(description);
    }

    public DeletingEntityException(final Exception cause)
    {
        super(cause);
    }

    public DeletingEntityException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
