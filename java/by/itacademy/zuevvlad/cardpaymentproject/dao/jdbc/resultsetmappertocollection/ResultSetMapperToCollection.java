package by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection;

import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetMappingToCollectionException;
import by.itacademy.zuevvlad.cardpaymentproject.dao.jdbc.resultsetmappertocollection.exception.ResultSetRowMappingToEntityException;
import by.itacademy.zuevvlad.cardpaymentproject.entity.Entity;

import java.sql.ResultSet;
import java.util.Collection;

public interface ResultSetMapperToCollection<TypeOfEntity extends Entity, TypeOfCollection extends Collection<? super TypeOfEntity>>     //TODO: заменить абстрактным классом с полем ResultSetRowMapperToEntity
{
    public abstract TypeOfCollection map(final ResultSet mappedResultSet)
            throws ResultSetMappingToCollectionException;
    public abstract ResultSetRowMapperToEntity<TypeOfEntity> getResultSetRowMapperToEntity();

    @FunctionalInterface
    public interface ResultSetRowMapperToEntity<TypeOfEntity extends Entity>
    {
        public abstract TypeOfEntity mapCurrentRow(final ResultSet resultSetOfMappedRow)
                throws ResultSetRowMappingToEntityException;
    }
}
