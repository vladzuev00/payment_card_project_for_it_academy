package by.itacademy.zuevvlad.cardpaymentproject.entity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.util.Objects;

public final class BankAccount extends Entity
{
    private BigDecimal money;
    private boolean blocked;

    public BankAccount()
    {
        super();

        this.money = BankAccount.VALUE_OF_NOT_DEFINED_MONEY;
        this.blocked = BankAccount.VALUE_OF_NOT_DEFINED_BLOCKED;
    }

    private static final BigDecimal VALUE_OF_NOT_DEFINED_MONEY = BigDecimal.ZERO;
    private static final boolean VALUE_OF_NOT_DEFINED_BLOCKED = false;

    public BankAccount(final long id)
    {
        super(id);

        this.money = BankAccount.VALUE_OF_NOT_DEFINED_MONEY;
        this.blocked = BankAccount.VALUE_OF_NOT_DEFINED_BLOCKED;
    }

    public BankAccount(final BigDecimal money, final boolean blocked)
    {
        super();

        this.money = money;
        this.blocked = blocked;
    }

    public BankAccount(final long id, final BigDecimal money, final boolean blocked)
    {
        super(id);

        this.money = money;
        this.blocked = blocked;
    }

    public final void setMoney(final BigDecimal money)
    {
        this.money = money;
    }

    public final BigDecimal getMoney()
    {
        return this.money;
    }

    public final void setBlocked(final boolean blocked)
    {
        this.blocked = blocked;
    }

    public final boolean isBlocked()
    {
        return this.blocked;
    }

    @Override
    public final boolean equals(final Object otherObject)
    {
        if(!super.equals(otherObject))
        {
            return false;
        }
        final BankAccount other = (BankAccount)otherObject;
        return Objects.equals(this.money, other.money) && this.blocked == other.blocked;
    }

    @Override
    public final int hashCode()
    {
        return super.hashCode() + Objects.hash(this.money, this.blocked);
    }

    @Override
    public final String toString()
    {
        return super.toString() + "[money = " + this.money + ", blocked = " + this.blocked + "]";
    }

    @Override
    public final void writeExternal(final ObjectOutput objectOutput)
            throws IOException
    {
        super.writeExternal(objectOutput);

        objectOutput.writeObject(this.money);
        objectOutput.writeBoolean(this.blocked);
    }

    @Override
    public final void readExternal(final ObjectInput objectInput)
            throws IOException, ClassNotFoundException
    {
        super.readExternal(objectInput);

        this.money = (BigDecimal)objectInput.readObject();
        this.blocked = objectInput.readBoolean();
    }
}
