package by.itacademy.zuevvlad.cardpaymentproject.dao.exception;

public final class AddingEntityException extends DAOException
{
    public AddingEntityException()
    {
        super();
    }

    public AddingEntityException(final String description)
    {
        super(description);
    }

    public AddingEntityException(final Exception cause)
    {
        super(cause);
    }

    public AddingEntityException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
