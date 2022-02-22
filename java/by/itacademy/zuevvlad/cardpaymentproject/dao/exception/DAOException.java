package by.itacademy.zuevvlad.cardpaymentproject.dao.exception;

public class DAOException extends Exception
{
    public DAOException()
    {
        super();
    }

    public DAOException(final String description)
    {
        super(description);
    }

    public DAOException(final Exception cause)
    {
        super(cause);
    }

    public DAOException(final String description, final Exception cause)
    {
        super(description, cause);
    }
}
