package by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty;

public enum BankAccountTableProperty
{
    NAME_OF_TABLE("bank_accounts"), NAME_OF_COLUMN_OF_ID("id"), NAME_OF_COLUMN_OF_MONEY("money"),
    NAME_OF_COLUMN_OF_BLOCKED("is_blocked");

    private final String value;

    private BankAccountTableProperty(final String value)
    {
        this.value = value;
    }

    public final String getValue()
    {
        return this.value;
    }
}
