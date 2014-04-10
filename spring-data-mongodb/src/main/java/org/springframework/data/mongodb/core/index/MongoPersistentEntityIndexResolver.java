/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mongodb.core.index.Index.Duplicates;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * {@link IndexResolver} implementation inspecting {@link MongoPersistentEntity} for {@link MongoPersistentEntity} to be
 * indexed.
 * 
 * @author Christoph Strobl
 * @since 1.5
 */
public class MongoPersistentEntityIndexResolver implements IndexResolver {

	private final MongoMappingContext mappingContext;

	/**
	 * @param mappingContext must not be {@literal null}.
	 */
	public MongoPersistentEntityIndexResolver(MongoMappingContext mappingContext) {

		Assert.notNull(mappingContext, "Mapping context must not be null in order to resolve index definitions");
		this.mappingContext = mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.index.IndexResolver#resolveIndexForClass(java.lang.Class)
	 */
	@Override
	public List<IndexDefinitionHolder> resolveIndexForClass(Class<?> type) {
		return resolveIndexForEntity(mappingContext.getPersistentEntity(type));
	}

	public List<IndexDefinitionHolder> resolveIndexForEntity(final MongoPersistentEntity<?> root) {

		Assert.notNull(root, "Index cannot be resolved for given 'null' entity.");
		Document document = root.findAnnotation(Document.class);
		Assert.notNull(document, "Given entity is not collection root.");

		final List<IndexDefinitionHolder> indexInformation = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions("", root.getCollection(), root.getType()));

		root.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				if (persistentProperty.isEntity()) {
					indexInformation.addAll(resolveIndexForClass(persistentProperty.getActualType(),
							persistentProperty.getFieldName(), root.getCollection()));
				}

				IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(
						persistentProperty.getFieldName(), root.getCollection(), persistentProperty);
				if (indexDefinitionHolder != null) {
					indexInformation.add(indexDefinitionHolder);
				}
			}
		});

		return indexInformation;
	}

	private List<IndexDefinitionHolder> resolveIndexForClass(Class<?> type, final String path, final String collection) {

		final List<IndexDefinitionHolder> indexInformation = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>();
		indexInformation.addAll(potentiallyCreateCompoundIndexDefinitions(path, collection, type));

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
		entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

				String propertyDotPath = (StringUtils.hasText(path) ? (path + ".") : "") + persistentProperty.getFieldName();

				if (persistentProperty.isEntity()) {
					indexInformation.addAll(resolveIndexForClass(persistentProperty.getActualType(), propertyDotPath, collection));
				}

				IndexDefinitionHolder indexDefinitionHolder = createIndexDefinitionHolderForProperty(propertyDotPath,
						collection, persistentProperty);
				if (indexDefinitionHolder != null) {
					indexInformation.add(indexDefinitionHolder);
				}
			}
		});

		return indexInformation;
	}

	private IndexDefinitionHolder createIndexDefinitionHolderForProperty(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		if (persistentProperty.isAnnotationPresent(Indexed.class)) {
			return createIndexDefinition(dotPath, collection, persistentProperty);
		} else if (persistentProperty.isAnnotationPresent(GeoSpatialIndexed.class)) {
			return createGeoSpatialIndexDefinition(dotPath, collection, persistentProperty);
		}

		return null;
	}

	private List<IndexDefinitionHolder> potentiallyCreateCompoundIndexDefinitions(String dotPath, String collection,
			Class<?> type) {

		if (AnnotationUtils.findAnnotation(type, CompoundIndexes.class) == null) {
			return Collections.emptyList();
		}

		return createCompoundIndexDefinitions(dotPath, collection, type);
	}

	/**
	 * Create {@link IndexDefinition} out {@link CompoundIndexes} of given type.
	 * 
	 * @param dotPath
	 * @param collection
	 * @param type
	 * @return
	 */
	protected List<IndexDefinitionHolder> createCompoundIndexDefinitions(String dotPath, String collection, Class<?> type) {

		CompoundIndexes indexes = AnnotationUtils.findAnnotation(type, CompoundIndexes.class);
		List<IndexDefinitionHolder> indexDefinitions = new ArrayList<MongoPersistentEntityIndexResolver.IndexDefinitionHolder>(
				indexes.value().length);

		for (CompoundIndex index : indexes.value()) {

			IndexDefinitionHolder holder = new IndexDefinitionHolder(StringUtils.hasText(index.name()) ? index.name()
					: dotPath);

			CompoundIndexDefinition indexDefinition = new CompoundIndexDefinition((DBObject) JSON.parse(index.def()));
			indexDefinition.named(index.name());
			indexDefinition.setCollection(StringUtils.hasText(index.collection()) ? index.collection() : collection);
			if (index.unique()) {
				indexDefinition.unique(index.dropDups() ? Duplicates.DROP : Duplicates.RETAIN);
			}
			if (index.sparse()) {
				indexDefinition.sparse();
			}
			if (index.background()) {
				indexDefinition.background();
			}
			if (index.expireAfterSeconds() >= 0) {
				indexDefinition.expire(index.expireAfterSeconds(), TimeUnit.SECONDS);
			}

			holder.setIndexDefinition(indexDefinition);
			indexDefinitions.add(holder);
		}

		return indexDefinitions;
	}

	/**
	 * Creates {@link IndexDefinition} out of {@link Indexed} for given {@link MongoPersistentProperty}.
	 * 
	 * @param dotPath
	 * @param collection
	 * @param persitentProperty
	 * @return
	 */
	protected IndexDefinitionHolder createIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persitentProperty) {

		Indexed index = persitentProperty.findAnnotation(Indexed.class);

		IndexDefinitionHolder holder = new IndexDefinitionHolder(dotPath);

		Index indexDefinition = new Index();
		indexDefinition.setCollection(StringUtils.hasText(index.collection()) ? index.collection() : collection);
		indexDefinition.named(StringUtils.hasText(index.name()) ? index.name() : persitentProperty.getFieldName());
		indexDefinition.on(persitentProperty.getFieldName(),
				IndexDirection.ASCENDING.equals(index.direction()) ? Sort.Direction.ASC : Sort.Direction.DESC);

		if (index.unique()) {
			indexDefinition.unique(index.dropDups() ? Duplicates.DROP : Duplicates.RETAIN);
		}
		if (index.sparse()) {
			indexDefinition.sparse();
		}
		if (index.background()) {
			indexDefinition.background();
		}
		if (index.expireAfterSeconds() >= 0) {
			indexDefinition.expire(index.expireAfterSeconds(), TimeUnit.SECONDS);
		}

		holder.setIndexDefinition(indexDefinition);
		return holder;
	}

	/**
	 * Creates {@link IndexDefinition} out of {@link GeoSpatialIndexed} for {@link MongoPersistentProperty}.
	 * 
	 * @param dotPath
	 * @param collection
	 * @param persistentProperty
	 * @return
	 */
	protected IndexDefinitionHolder createGeoSpatialIndexDefinition(String dotPath, String collection,
			MongoPersistentProperty persistentProperty) {

		GeoSpatialIndexed index = persistentProperty.findAnnotation(GeoSpatialIndexed.class);

		IndexDefinitionHolder holder = new IndexDefinitionHolder(dotPath);

		GeospatialIndex indexDefinition = new GeospatialIndex(dotPath);
		indexDefinition.setCollection(StringUtils.hasText(index.collection()) ? index.collection() : collection);
		indexDefinition.withBits(index.bits());
		indexDefinition.withMin(index.min()).withMax(index.max());
		indexDefinition.named(StringUtils.hasText(index.name()) ? index.name() : persistentProperty.getName());
		indexDefinition.typed(index.type()).withBucketSize(index.bucketSize()).withAdditionalField(index.additionalField());

		holder.setIndexDefinition(indexDefinition);
		return holder;
	}

	public static class IndexDefinitionHolder implements IndexDefinition {

		private String path;
		private IndexDefinition indexDefinition;

		public IndexDefinitionHolder(String path) {
			this.path = path;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getCollection()
		 */
		@Override
		public String getCollection() {
			return indexDefinition != null ? indexDefinition.getCollection() : null;
		}

		public String getPath() {
			return path;
		}

		public IndexDefinition getIndexDefinition() {
			return indexDefinition;
		}

		public void setIndexDefinition(IndexDefinition indexDefinition) {
			this.indexDefinition = indexDefinition;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getIndexKeys()
		 */
		@Override
		public DBObject getIndexKeys() {
			return indexDefinition.getIndexKeys();
		}

		@Override
		public DBObject getIndexOptions() {
			return indexDefinition.getIndexOptions();
		}

	}

}
