package by.itacademy.zuevvlad.cardpaymentproject.entity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

public final class Client extends User
{
    private String name;
    private String surname;
    private String patronymic;
    private String phoneNumber;
    private BankAccount bankAccount;

    public Client()
    {
        super();

        this.name = Client.VALUE_OF_NOT_DEFINED_NAME;
        this.surname = Client.VALUE_OF_NOT_DEFINED_SURNAME;
        this.patronymic = Client.VALUE_OF_NOT_DEFINED_PATRONYMIC;
        this.phoneNumber = Client.VALUE_OF_NOT_DEFINED_PHONE_NUMBER;
        this.bankAccount = new BankAccount();
    }

    private static final String VALUE_OF_NOT_DEFINED_NAME = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_SURNAME = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_PATRONYMIC = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_PHONE_NUMBER = "not defined";

    public Client(final long id, final String email, final String password)
    {
        super(id, email, password);

        this.name = Client.VALUE_OF_NOT_DEFINED_NAME;
        this.surname = Client.VALUE_OF_NOT_DEFINED_SURNAME;
        this.patronymic = Client.VALUE_OF_NOT_DEFINED_PATRONYMIC;
        this.phoneNumber = Client.VALUE_OF_NOT_DEFINED_PHONE_NUMBER;
        this.bankAccount = new BankAccount();
    }

    public Client(final String name, final String surname, final String patronymic, final String phoneNumber,
                  final BankAccount bankAccount)
    {
        super();

        this.name = name;
        this.surname = surname;
        this.patronymic = patronymic;
        this.phoneNumber = phoneNumber;
        this.bankAccount = bankAccount;
    }

    public Client(final long id, final String email, final String password, final String name, final String surname,
                  final String patronymic, final String phoneNumber, final BankAccount bankAccount)
    {
        super(id, email, password);

        this.name = name;
        this.surname = surname;
        this.patronymic = patronymic;
        this.phoneNumber = phoneNumber;
        this.bankAccount = bankAccount;
    }

    public final void setName(final String name)
    {
        this.name = name;
    }

    public final String getName()
    {
        return this.name;
    }

    public final void setSurname(final String surname)
    {
        this.surname = surname;
    }

    public final String getSurname()
    {
        return this.surname;
    }

    public final void setPatronymic(final String patronymic)
    {
        this.patronymic = patronymic;
    }

    public final String getPatronymic()
    {
        return this.patronymic;
    }

    public final void setPhoneNumber(final String phoneNumber)
    {
        this.phoneNumber = phoneNumber;
    }

    public final String getPhoneNumber()
    {
        return this.phoneNumber;
    }

    public final void setBankAccount(final BankAccount bankAccount)
    {
        this.bankAccount = bankAccount;
    }

    public final BankAccount getBankAccount()
    {
        return this.bankAccount;
    }

    @Override
    public final boolean equals(final Object otherObject)
    {
        if(!super.equals(otherObject))
        {
            return false;
        }
        final Client other = (Client)otherObject;
        return     Objects.equals(this.name, other.name)
                && Objects.equals(this.surname, other.surname)
                && Objects.equals(this.patronymic, other.patronymic)
                && Objects.equals(this.phoneNumber, other.phoneNumber)
                && Objects.equals(this.bankAccount, other.bankAccount);
    }

    @Override
    public final int hashCode()
    {
        return super.hashCode() + Objects.hash(this.name, this.surname, this.patronymic, this.phoneNumber,
                this.bankAccount);
    }

    @Override
    public final String toString()
    {
        return super.toString() + "[name = " + this.name + ", surname = " + this.surname
                + ", patronymic = " + this.patronymic + ", phoneNumber = " + this.phoneNumber
                + ", bankAccount = " + this.bankAccount + "]";
    }

    @Override
    public final void writeExternal(final ObjectOutput objectOutput)
            throws IOException
    {
        super.writeExternal(objectOutput);

        objectOutput.writeObject(this.name);
        objectOutput.writeObject(this.surname);
        objectOutput.writeObject(this.patronymic);
        objectOutput.writeObject(this.phoneNumber);
        objectOutput.writeObject(this.bankAccount);
    }

    @Override
    public final void readExternal(final ObjectInput objectInput)
            throws IOException, ClassNotFoundException
    {
        super.readExternal(objectInput);

        this.name = (String)objectInput.readObject();
        this.surname = (String)objectInput.readObject();
        this.patronymic = (String)objectInput.readObject();
        this.phoneNumber = (String)objectInput.readObject();
        this.bankAccount = (BankAccount)objectInput.readObject();
    }
}

