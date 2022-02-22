package by.itacademy.zuevvlad.cardpaymentproject.dao.exception;

public final class FindingEntityException extends DAOException
{
    public FindingEntityException()
    {
        super();
    }

    public FindingEntityException(final String description)
    {
        super(description);
    }

    public FindingEntityException(final Exception cause)
    {
        super(cause);
    }

    public FindingEntityException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
