package by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty;

public enum UserTableProperty
{
    NAME_OF_TABLE("users"), NAME_OF_COLUMN_OF_ID("id"), NAME_OF_COLUMN_OF_EMAIL("email"),
    NAME_OF_COLUMN_OF_ENCRYPTED_PASSWORD("encrypted_password");

    private final String value;

    private UserTableProperty(final String value)
    {
        this.value = value;
    }

    public final String getValue()
    {
        return this.value;
    }
}
