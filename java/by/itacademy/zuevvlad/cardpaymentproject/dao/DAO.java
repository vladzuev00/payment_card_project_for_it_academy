package by.itacademy.zuevvlad.cardpaymentproject.dao;

import by.itacademy.zuevvlad.cardpaymentproject.dao.exception.*;
import by.itacademy.zuevvlad.cardpaymentproject.entity.Entity;

import java.util.Optional;
import java.util.stream.Stream;

public interface DAO<TypeOfStoredEntity extends Entity>
{
    public abstract void addEntity(final TypeOfStoredEntity addedEntity)
            throws AddingEntityException;
    public abstract Iterable<TypeOfStoredEntity> offloadEntities()
            throws OffloadingEntitiesException;
    public abstract Optional<TypeOfStoredEntity> findEntityById(final long idOfFoundEntity)
            throws FindingEntityException;
    public abstract void updateEntity(final TypeOfStoredEntity updatedEntity)
            throws UpdatingEntityException;
    public abstract void deleteEntityById(final long idOfDeletedEntity)
            throws DeletingEntityException;

    public default void deleteEntity(final TypeOfStoredEntity deletedEntity)
            throws DeletingEntityException
    {
        this.deleteEntityById(deletedEntity.getId());
    }

    public abstract boolean isEntityWithGivenIdExisting(final long idOfResearchEntity)
            throws DefiningExistingEntityException;
}