/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ostridm.kafka.connect.transforms;


import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class MappedTranslate<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Performs mapping of the field value using the configured mappings or fallback string.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Source field name for transform")

            .define(ConfigName.MAP_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Mappings configuration field for transform, comma separated list")

            .define(ConfigName.DEFAULT_VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Default fallback string.");

    public static final ConfigDef CHILD_CONFIG_DEF = new ConfigDef()
            .define(ConfigName.CHILD_KEY_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.HIGH, "Key, default value equeals to $alias")
            .define(ConfigName.CHILD_VALUES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "List of items to search");

    private static final String PURPOSE = "rewriting field content";

    private String config_field_name;

    private String config_default_value;
    private Cache<Schema, Schema> schemaUpdateCache;
    private Map<String, String> config_mappings;

    @Override
    public void configure(Map<String, ?> props) {

        config_mappings = new HashMap<>();

        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        config_field_name = config.getString(ConfigName.FIELD_CONFIG);
        config_default_value = config.getString(ConfigName.DEFAULT_VALUE_CONFIG);

        List<String> maps = config.getList(ConfigName.MAP_CONFIG);

        for (String alias : maps) {
            SimpleConfig childConfig;
            try {
                childConfig = new SimpleConfig(CHILD_CONFIG_DEF, config.originalsWithPrefix(ConfigName.MAP_CONFIG + "." + alias + "."));
            } catch (ConfigException e) {
                throw new ConfigException("Wrong config for alias: " + alias, e);
            }

            String key = childConfig.getString(ConfigName.CHILD_KEY_CONFIG);
            List<String> values = childConfig.getList(ConfigName.CHILD_VALUES_CONFIG);

            if (null == key || key.isEmpty()) {
                key = alias;
            }
            for (String v : values) {
                config_mappings.put(v, key);
            }

        }

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public Schema getTypeSchema(boolean isOptional) {
        return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
    }

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        if (this.config_field_name.isEmpty()) {
            Object value = operatingValue(record);
            Schema updatedSchema = getTypeSchema(schema.isOptional());
            return newRecord(record, updatedSchema, mapValue(value));
        } else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                for (Field field : schema.fields()) {
                    if (field.name().equals(this.config_field_name)) {
                        builder.field(field.name(), getTypeSchema(field.schema().isOptional()));
                    } else {
                        builder.field(field.name(), field.schema());
                    }
                }
                if (schema.isOptional())
                    builder.optional();
                if (schema.defaultValue() != null) {
                    Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                    builder.defaultValue(updatedDefaultValue);
                }

                updatedSchema = builder.build();
                schemaUpdateCache.put(schema, updatedSchema);
            }

            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(this.config_field_name)) {
                updatedFieldValue = mapValue(value.get(field));
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private R applySchemaless(R record) {
        Object rawValue = operatingValue(record);
        if (rawValue == null || this.config_field_name.isEmpty()) {
            return newRecord(record, null, mapValue(rawValue));
        } else {
            final Map<String, Object> value = requireMap(rawValue, PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            updatedValue.put(this.config_field_name, mapValue(value.get(this.config_field_name)));
            return newRecord(record, null, updatedValue);
        }
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     *
     * @param field         the input field, may be null
     * @param map           the mapping container
     * @param default_value default if replacement regex is not matched, may be null
     * @return the converted field
     */
    private Object mapValue(Object field, Map map, String default_value) {

        if (field == null) {
            return default_value;
        }

        String value = field.toString();

        if (map.containsKey(value)) {
            return map.get(value);
        }

        return default_value;
    }

    private Object mapValue(Object field) {
        return mapValue(field, this.config_mappings, this.config_default_value);
    }

    public interface ConfigName {
        String FIELD_CONFIG = "field";
        String DEFAULT_VALUE_CONFIG = "default";
        String MAP_CONFIG = "map";
        String CHILD_KEY_CONFIG = "key";
        String CHILD_VALUES_CONFIG = "values";
    }

    public static class Key<R extends ConnectRecord<R>> extends MappedTranslate<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends MappedTranslate<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
