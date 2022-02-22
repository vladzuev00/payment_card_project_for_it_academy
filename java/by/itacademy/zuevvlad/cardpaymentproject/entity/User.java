package by.itacademy.zuevvlad.cardpaymentproject.entity;

import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.Cryptographer;
import by.itacademy.zuevvlad.cardpaymentproject.service.cryptographer.StringToStringCryptographer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

public class User extends Entity
{
    private String email;
    private String password;

    private final Cryptographer<String, String> cryptographer;

    public User()
    {
        super();

        this.email = User.VALUE_OF_NOT_DEFINED_EMAIL;
        this.password = User.VALUE_OF_NOT_DEFINED_PASSWORD;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    private static final String VALUE_OF_NOT_DEFINED_EMAIL = "not defined";
    private static final String VALUE_OF_NOT_DEFINED_PASSWORD = "not defined";

    public User(final long id)
    {
        super(id);

        this.email = User.VALUE_OF_NOT_DEFINED_EMAIL;
        this.password = User.VALUE_OF_NOT_DEFINED_PASSWORD;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    public User(final String email, final String password)
    {
        super();

        this.email = email;
        this.password = password;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    public User(final long id, final String email, final String password)
    {
        super(id);

        this.email = email;
        this.password = password;

        this.cryptographer = StringToStringCryptographer.createCryptographer();
    }

    public final void setEmail(final String email)
    {
        this.email = email;
    }

    public final String getEmail()
    {
        return this.email;
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
    public boolean equals(final Object otherObject)
    {
        if(!super.equals(otherObject))
        {
            return false;
        }
        final User other = (User)otherObject;
        return Objects.equals(this.email, other.email) && Objects.equals(this.password, other.password);
    }

    @Override
    public int hashCode()
    {
        return super.hashCode() + Objects.hash(this.email, this.password);
    }

    @Override
    public String toString()
    {
        return super.toString() + "[email = " + this.email + ", password = " + this.password + "]";
    }

    @Override
    public void writeExternal(final ObjectOutput objectOutput)
            throws IOException
    {
        super.writeExternal(objectOutput);

        objectOutput.writeObject(this.email);

        final String encryptedPassword = this.cryptographer.encrypt(this.password);
        objectOutput.writeObject(encryptedPassword);
    }

    @Override
    public void readExternal(final ObjectInput objectInput)
            throws IOException, ClassNotFoundException
    {
        super.readExternal(objectInput);

        this.email = (String)objectInput.readObject();

        final String encryptedPassword = (String)objectInput.readObject();
        this.password = this.cryptographer.decrypt(encryptedPassword);
    }
}
