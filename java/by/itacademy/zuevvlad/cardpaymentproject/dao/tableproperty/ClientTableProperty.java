package by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty;

public enum ClientTableProperty
{
    NAME_OF_TABLE("clients"), NAME_OF_COLUMN_OF_NAME("name"), NAME_OF_COLUMN_OF_SURNAME("surname"),
    NAME_OF_COLUMN_OF_PATRONYMIC("patronymic"), NAME_OF_COLUMN_OF_PHONE_NUMBER("phone_number"),
    NAME_OF_COLUMN_OF_BANK_ACCOUNT_ID("bank_account_id"), NAME_OF_COLUMN_OF_ID("id");

    private final String value;

    private ClientTableProperty(final String value)
    {
        this.value = value;
    }

    public final String getValue()
    {
        return this.value;
    }
}
