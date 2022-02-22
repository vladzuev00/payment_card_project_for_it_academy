package by.itacademy.zuevvlad.cardpaymentproject.entity;

import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Month;
import java.time.Year;
import java.util.Objects;

public final class PaymentCard extends Entity
{
    private String cardNumber;
    private PaymentCard.ExpirationDate expirationDate;
    private String paymentSystem;
    private String cvc;
    private Client client;
    private String nameOfBank;
    private String password;

    private final Cryptographer<String, String> cryptographer;

    public PaymentCard()
    {
        super();

        this.cardNumber = PaymentCard.VALUE_OF_NOT_DEFINED_CARD_NUMBER;
        this.expirationDate = new PaymentCard.ExpirationDate();
        this.paymentSystem = PaymentCard.VALUE_OF_NOT_DEFINED_PAYMENT_SYSTEM;
        this.cvc = PaymentCard.VALUE_OF_NOT_DEFINED_CVC;
        this.client = new Client();
        this.nameOfBank = PaymentCard.VALUE_OF_NOT_DEFINED_NAME_OF_BANK;
        this.password = PaymentCard.VALUE_OF_NOT_DEFINED_PASSWORD;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    private static final String VALUE_OF_NOT_DEFINED_CARD_NUMBER = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_PAYMENT_SYSTEM = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_CVC = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_NAME_OF_BANK = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_PASSWORD = "not defined";

    public PaymentCard(final long id)
    {
        super(id);

        this.cardNumber = PaymentCard.VALUE_OF_NOT_DEFINED_CARD_NUMBER;
        this.expirationDate = new PaymentCard.ExpirationDate();
        this.paymentSystem = PaymentCard.VALUE_OF_NOT_DEFINED_PAYMENT_SYSTEM;
        this.cvc = PaymentCard.VALUE_OF_NOT_DEFINED_CVC;
        this.client = new Client();
        this.nameOfBank = PaymentCard.VALUE_OF_NOT_DEFINED_NAME_OF_BANK;
        this.password = PaymentCard.VALUE_OF_NOT_DEFINED_PASSWORD;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    public PaymentCard(final String cardNumber, final PaymentCard.ExpirationDate expirationDate,
                       final String paymentSystem, final String cvc, final Client client, final String nameOfBank,
                       final String password)
    {
        super();
        this.cardNumber = cardNumber;
        this.expirationDate = expirationDate;
        this.paymentSystem = paymentSystem;
        this.cvc = cvc;
        this.client = client;
        this.nameOfBank = nameOfBank;
        this.password = password;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    public PaymentCard(final long id, final String cardNumber, final PaymentCard.ExpirationDate expirationDate,
                       final String paymentSystem, final String cvc, final Client client, final String nameOfBank,
                       final String password)
    {
        super(id);

        this.cardNumber = cardNumber;
        this.expirationDate = expirationDate;
        this.paymentSystem = paymentSystem;
        this.cvc = cvc;
        this.client = client;
        this.nameOfBank = nameOfBank;
        this.password = password;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    public final void setCardNumber(final String cardNumber)
    {
        this.cardNumber = cardNumber;
    }

    public final String getCardNumber()
    {
        return this.cardNumber;
    }

    public final void setExpirationDate(final PaymentCard.ExpirationDate expirationDate)
    {
        this.expirationDate = expirationDate;
    }

    public final PaymentCard.ExpirationDate getExpirationDate()
    {
        return this.expirationDate;
    }

    public final void setPaymentSystem(final String paymentSystem)
    {
        this.paymentSystem = paymentSystem;
    }

    public final String getPaymentSystem()
    {
        return this.paymentSystem;
    }

    public final void setCvc(final String cvc)
    {
        this.cvc = cvc;
    }

    public final String getCvc()
    {
        return this.cvc;
    }

    public final void setClient(final Client client)
    {
        this.client = client;
    }

    public final Client getClient()
    {
        return this.client;
    }

    public final void setNameOfBank(final String nameOfBank)
    {
        this.nameOfBank = nameOfBank;
    }

    public final String getNameOfBank()
    {
        return this.nameOfBank;
    }

    public final void setPassword(final String password)
    {
        this.password = password;
    }

    public final String getPassword()
    {
        return this.password;
    }

    @Override
    public final boolean equals(final Object otherObject)
    {
        if(!super.equals(otherObject))
        {
            return false;
        }
        final PaymentCard other = (PaymentCard)otherObject;
        return     Objects.equals(this.cardNumber, other.cardNumber)
                && Objects.equals(this.expirationDate, other.expirationDate)
                && Objects.equals(this.paymentSystem, other.paymentSystem)
                && Objects.equals(this.cvc, other.cvc)
                && Objects.equals(this.client, other.client)
                && Objects.equals(this.nameOfBank, other.nameOfBank)
                && Objects.equals(this.password, other.password);
    }

    @Override
    public final int hashCode()
    {
        return super.hashCode() + Objects.hash(this.cardNumber, this.expirationDate, this.paymentSystem, this.cvc,
                this.client, this.nameOfBank, this.password);
    }

    @Override
    public final String toString()
    {
        return super.toString() + "[cardNumber = " + this.cardNumber + ", expirationDate = " + this.expirationDate
                + ", paymentSystem = " + this.paymentSystem + ", cvc = " + this.cvc + ", client = " + this.client
                + ", nameOfBank = " + this.nameOfBank + ", password = " + this.password + "]";
    }

    @Override
    public final void writeExternal(final ObjectOutput objectOutput)
            throws IOException
    {
        super.writeExternal(objectOutput);

        objectOutput.writeObject(this.cardNumber);
        objectOutput.writeObject(this.expirationDate);
        objectOutput.writeObject(this.paymentSystem);

        final String encryptedCvc = this.cryptographer.encrypt(this.cvc);
        objectOutput.writeObject(encryptedCvc);

        objectOutput.writeObject(this.client);
        objectOutput.writeObject(this.nameOfBank);

        final String encryptedPassword = this.cryptographer.encrypt(this.password);
        objectOutput.writeObject(encryptedPassword);
    }

    @Override
    public final void readExternal(final ObjectInput objectInput)
            throws IOException, ClassNotFoundException
    {
        super.readExternal(objectInput);

        this.cardNumber = (String)objectInput.readObject();
        this.expirationDate = (PaymentCard.ExpirationDate)objectInput.readObject();
        this.paymentSystem = (String)objectInput.readObject();

        final String encryptedCvs = (String)objectInput.readObject();
        this.cvc = this.cryptographer.decrypt(encryptedCvs);

        this.client = (Client)objectInput.readObject();
        this.nameOfBank = (String)objectInput.readObject();

        final String encryptedPassword = (String)objectInput.readObject();
        this.password = this.cryptographer.decrypt(encryptedPassword);
    }

    public static final class ExpirationDate implements Externalizable
    {
        private Month month;
        private Year year;

        public ExpirationDate()
        {
            super();

            this.month = ExpirationDate.VALUE_OF_NOT_DEFINED_MONTH;
            this.year = ExpirationDate.VALUE_OF_NOT_DEFINED_YEAR;
        }

        private static final Month VALUE_OF_NOT_DEFINED_MONTH = Month.JANUARY;
        private static final Year VALUE_OF_NOT_DEFINED_YEAR = Year.of(Year.MIN_VALUE);

        public ExpirationDate(final Month month, final Year year)
        {
            super();

            this.month = month;
            this.year = year;
        }

        public final void setMonth(final Month month)
        {
            this.month = month;
        }

        public final Month getMonth()
        {
            return this.month;
        }

        public final void setYear(final Year year)
        {
            this.year = year;
        }

        public final Year getYear()
        {
            return this.year;
        }

        @Override
        public final boolean equals(final Object otherObject)
        {
            if(this == otherObject)
            {
                return true;
            }
            if(otherObject == null)
            {
                return false;
            }
            if(this.getClass() != otherObject.getClass())
            {
                return false;
            }
            final ExpirationDate other = (ExpirationDate)otherObject;
            return Objects.equals(this.month, other.month) && Objects.equals(this.year, other.year);
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(this.month, this.year);
        }

        @Override
        public final String toString()
        {
            return this.getClass().getName() + "[month " + this.month + ", year = " + this.year + "]";
        }

        @Override
        public final void writeExternal(final ObjectOutput objectOutput)
                throws IOException
        {
            objectOutput.writeObject(this.month);
            objectOutput.writeObject(this.year);
        }

        @Override
        public final void readExternal(final ObjectInput objectInput)
                throws IOException, ClassNotFoundException
        {
            this.month = (Month)objectInput.readObject();
            this.year = (Year)objectInput.readObject();
        }
    }
}
