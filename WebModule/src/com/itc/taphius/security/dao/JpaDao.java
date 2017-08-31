package com.itc.taphius.security.dao;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;


import org.springframework.transaction.annotation.Transactional;

import com.itc.taphius.security.entity.Entity;


public class JpaDao<T extends Entity, I> implements Dao<T, I>
{

	private EntityManager entityManager;

	protected Class<T> entityClass;


	public JpaDao(Class<T> entityClass)
	{
		this.entityClass = entityClass;
	}


	public EntityManager getEntityManager()
	{
		return this.entityManager;
	}


	@PersistenceContext
	public void setEntityManager(final EntityManager entityManager)
	{
		this.entityManager = entityManager;
	}


	@Override
	@Transactional(readOnly = true)
	public List<T> findAll()
	{
		final CriteriaBuilder builder = this.getEntityManager().getCriteriaBuilder();
		final CriteriaQuery<T> criteriaQuery = builder.createQuery(this.entityClass);

		criteriaQuery.from(this.entityClass);

		TypedQuery<T> typedQuery = this.getEntityManager().createQuery(criteriaQuery);
		List<T> results = typedQuery.getResultList();
		this.getEntityManager().close();
		return results;
	}


	@Override
	@Transactional(readOnly = true)
	public T find(I id)
	{
		T user = this.getEntityManager().find(this.entityClass, id);
		this.getEntityManager().close();
		return user;
	}


	@Override
	@Transactional
	public T save(T entity)
	{
		return this.getEntityManager().merge(entity);
	}


	@Override
	@Transactional
	public void delete(I id)
	{
		if (id == null) {
			return;
		}

		T entity = this.find(id);
		if (entity == null) {
			return;
		}

		this.getEntityManager().remove(entity);
	}

}