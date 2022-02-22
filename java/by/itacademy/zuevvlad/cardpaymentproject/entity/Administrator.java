package by.itacademy.zuevvlad.cardpaymentproject.entity;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

public final class Administrator extends User
{
    private Administrator.Level level;

    public Administrator()
    {
        super();

        this.level = Level.NOT_DEFINED;
    }

    public Administrator(final long id, final String email, final String password)
    {
        super(id, email, password);

        this.level = Level.NOT_DEFINED;
    }

    public Administrator(final long id, final String email, final String password, final Administrator.Level level)
    {
        super(id, email, password);

        this.level = level;
    }

    public final void setLevel(final Administrator.Level level)
    {
        this.level = level;
    }

    public final Administrator.Level getLevel()
    {
        return this.level;
    }

    @Override
    public final boolean equals(final Object otherObject)
    {
        if(!super.equals(otherObject))
        {
            return false;
        }
        final Administrator other = (Administrator)otherObject;
        return this.level == other.level;
    }

    @Override
    public final int hashCode()
    {
        return super.hashCode() + Objects.hashCode(this.level);
    }

    @Override
    public final String toString()
    {
        return super.toString() + "[level = " + this.level + "]";
    }

    @Override
    public final void writeExternal(final ObjectOutput objectOutput)
            throws IOException
    {
        super.writeExternal(objectOutput);

        objectOutput.writeObject(this.level);
    }

    @Override
    public final void readExternal(final ObjectInput objectInput)
            throws IOException, ClassNotFoundException
    {
        super.readExternal(objectInput);

        this.level = (Administrator.Level)objectInput.readObject();
    }

    public static enum Level
    {
        NOT_DEFINED, SUPPORTER, MODIFIER, MAIN
    }
}

