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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RegexRewriteTest {

    private final String ORIGINAL_VALUE = "a48a9f5f8efee17aaa1e75123a4cbfe2322db3f3";
    private final String EXPECTED_TRANSFORM_VALUE = "{\"wow\":\"a48a9f5f8efee17aaa1e75123a4cbfe2322db3f3\"}";

    private RegexRewrite<SourceRecord> transform = new RegexRewrite.Value<>();

    @After
    public void tearDown() throws Exception {
        transform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {

        final Map<String, Object> props = getTestProps();

        transform.configure(props);

        transform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndRegexRewriteField() {
        final Map<String, Object> props = getTestProps();

        transform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("hash", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("hash", ORIGINAL_VALUE);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = transform.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("hash").schema());

        final Object value = ((Struct) transformedRecord.value()).getString("hash");
        assertNotNull(value);
        assertEquals(value, EXPECTED_TRANSFORM_VALUE);

        // Exercise caching
        final SourceRecord transformedRecord2 = transform.apply(
                new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

    }

    @Test
    public void schemalessRegexRewriteField() {
        final Map<String, Object> props = getTestProps();

        transform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Collections.singletonMap("hash", ORIGINAL_VALUE));

        final SourceRecord transformedRecord = transform.apply(record);
        assertEquals(EXPECTED_TRANSFORM_VALUE, ((Map) transformedRecord.value()).get("hash"));
    }

    private Map<String, Object> getTestProps() {
        final Map<String, Object> props = new HashMap<>();

        props.put("field", "hash");
        props.put("regex", "([0-9a-f]{40}|)");
        props.put("replacement", "{\"wow\":\"$1\"}");
        props.put("default", "{\"wow\":\"\"");
        return props;
    }
}