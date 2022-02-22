package by.itacademy.zuevvlad.cardpaymentproject.dao.tableproperty;

public enum AdministratorTableProperty
{
    NAME_OF_TABLE("administrators"), NAME_OF_COLUMN_OF_ID("id"), NAME_OF_COLUMN_OF_LEVEL("level");

    private final String value;

    private AdministratorTableProperty(final String value)
    {
        this.value = value;
    }

    public final String getValue()
    {
        return this.value;
    }
}
