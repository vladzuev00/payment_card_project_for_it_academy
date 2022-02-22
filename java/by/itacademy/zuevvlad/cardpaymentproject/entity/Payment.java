package by.itacademy.zuevvlad.cardpaymentproject.entity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.util.Objects;

public final class Payment extends Entity
{
    private PaymentCard cardOfSender;
    private PaymentCard cardOfReceiver;
    private BigDecimal money;

    public Payment()
    {
        super();

        this.cardOfSender = new PaymentCard();
        this.cardOfReceiver = new PaymentCard();
        this.money = Payment.VALUE_OF_NOT_DEFINED_MONEY;
    }

    private static final BigDecimal VALUE_OF_NOT_DEFINED_MONEY = BigDecimal.ZERO;

    public Payment(final long id)
    {
        super(id);

        this.cardOfSender = new PaymentCard();
        this.cardOfReceiver = new PaymentCard();
        this.money = Payment.VALUE_OF_NOT_DEFINED_MONEY;
    }

    public Payment(final PaymentCard cardOfSender, final PaymentCard cardOfReceiver, final BigDecimal money)
    {
        super();

        this.cardOfSender = cardOfSender;
        this.cardOfReceiver = cardOfReceiver;
        this.money = money;
    }

    public Payment(final long id, final PaymentCard cardOfSender, final PaymentCard cardOfReceiver,
                   final BigDecimal money)
    {
        super(id);

        this.cardOfSender = cardOfSender;
        this.cardOfReceiver = cardOfReceiver;
        this.money = money;
    }

    public final void setCardOfSender(final PaymentCard cardOfSender)
    {
        this.cardOfSender = cardOfSender;
    }

    public final PaymentCard getCardOfSender()
    {
        return this.cardOfSender;
    }

    public final void setCardOfReceiver(final PaymentCard cardOfReceiver)
    {
        this.cardOfReceiver = cardOfReceiver;
    }

    public final PaymentCard getCardOfReceiver()
    {
        return this.cardOfReceiver;
    }

    @Override
    public final boolean equals(final Object otherObject)
    {
        if(!super.equals(otherObject))
        {
            return false;
        }
        final Payment other = (Payment)otherObject;
        return     Objects.equals(this.cardOfSender, other.cardOfSender)
                && Objects.equals(this.cardOfReceiver, other.cardOfReceiver)
                && Objects.equals(this.money, other.money);
    }

    @Override
    public final int hashCode()
    {
        return super.hashCode() + Objects.hash(this.cardOfSender, this.cardOfReceiver, this.money);
    }

    @Override
    public final String toString()
    {
        return super.toString() + "[cardOfSender = " + this.cardOfSender + ", cardOfReceiver = " + this.cardOfReceiver
                + ", money = " + this.money + "]";
    }

    @Override
    public final void writeExternal(final ObjectOutput objectOutput)
            throws IOException
    {
        super.writeExternal(objectOutput);

        objectOutput.writeObject(this.cardOfSender);
        objectOutput.writeObject(this.cardOfReceiver);
        objectOutput.writeObject(this.money);
    }

    @Override
    public final void readExternal(final ObjectInput objectInput)
            throws IOException, ClassNotFoundException
    {
        super.readExternal(objectInput);

        this.cardOfSender = (PaymentCard)objectInput.readObject();
        this.cardOfReceiver = (PaymentCard)objectInput.readObject();
        this.money = (BigDecimal)objectInput.readObject();
    }
}
