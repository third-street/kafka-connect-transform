package com.thirdstreet.transform;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import com.thirdstreet.transform.CharTrimTransform;
import com.thirdstreet.transform.CharTrimTransformConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CharTrimTransformTest {

    public static final String ORIGINAL_VALUE       = "\"123456123789\"";
    public static final String EXPECTED_FIRST_VALUE = "123456123789";
    public static final String EXPECTED_ALL_VALUE   = "123456123789";
    public static final String FIELD_NAME           = "fieldName";
    public static final String OTHER_FIELD_NAME     = "otherFieldName";
    public static final long OTHER_FIELD_VALUE     = 123L;
//    public static final String UNCHANGED_VALUE      = "unchangedValue";

    private final CharTrimTransform<SourceRecord> xformKey = new CharTrimTransform.Key<>();
    private final CharTrimTransform<SourceRecord> xformValue = new CharTrimTransform.Value<>();

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test(expected = ConfigException.class)
    public void testConfigNoRegex() {
        xformValue.configure(Collections.<String, String>emptyMap());
    }

    @Test(expected = ConfigException.class)
    public void testConfigNoValidReplacementMethod() {
        xformValue.configure(Collections.singletonMap(CharTrimTransformConfig.CHARACTER_CONFIG, "invalid-value"));
    }

    @Test
    public void testSchemalessEntireValueReplaceFirst() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformValue.configure(config);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, ORIGINAL_VALUE));
        assertThat(transformed).isNotNull();
        assertThat(transformed.value()).isEqualTo(EXPECTED_FIRST_VALUE);
    }

    @Test
    public void testSchemalessEntireValueReplaceAll() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformValue.configure(config);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, ORIGINAL_VALUE));
        assertThat(transformed).isNotNull();
        assertThat(transformed.value()).isEqualTo(EXPECTED_ALL_VALUE);
    }

    @Test
    public void testSchemalessEntireKeyReplaceFirst() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformKey.configure(config);

        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", null, ORIGINAL_VALUE, null, null));
        assertThat(transformed).isNotNull();
        assertThat(transformed.key()).isEqualTo(EXPECTED_FIRST_VALUE);
    }

    @Test
    public void testSchemalessEntireKeyReplaceAll() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformKey.configure(config);

        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", null, ORIGINAL_VALUE, null, null));
        assertThat(transformed).isNotNull();
        assertThat(transformed.key()).isEqualTo(EXPECTED_ALL_VALUE);
    }

    @Test
    public void testSchemalessAllValuesKeyReplaceFirst() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformKey.configure(config);

        Map<String, Object> key = new HashMap<>();
        key.put(FIELD_NAME, ORIGINAL_VALUE);
        key.put(OTHER_FIELD_NAME, OTHER_FIELD_VALUE);

        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", null, key, null, null));
        assertThat(transformed).isNotNull();
        assertThat(transformed.key()).isNotNull();
        Map transformedKey = (Map) transformed.key();
        assertThat(transformedKey.size()).isEqualTo(2);
        assertThat(transformedKey).containsKey(FIELD_NAME);
        assertThat(transformedKey).containsValue(EXPECTED_FIRST_VALUE);
        assertThat(transformedKey).doesNotContainValue(ORIGINAL_VALUE);

        assertThat(transformedKey).containsKey(OTHER_FIELD_NAME);
        assertThat(transformedKey).containsValue(OTHER_FIELD_VALUE);
    }

    @Test
    public void testSchemalessAllValuesKeyReplace() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.FIELD_CONFIG, FIELD_NAME);
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformValue.configure(config);

        Map<String, Object> value = new HashMap<>();
        value.put(FIELD_NAME, ORIGINAL_VALUE);
        value.put(OTHER_FIELD_NAME, OTHER_FIELD_VALUE);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", null, null, value));
        assertThat(transformed).isNotNull();
        assertThat(transformed.value()).isNotNull();
        Map transformedValue = (Map) transformed.value();
        assertThat(transformedValue.size()).isEqualTo(2);
        assertThat(transformedValue).containsKey(FIELD_NAME);
        assertThat(transformedValue).containsValue(EXPECTED_FIRST_VALUE);
        assertThat(transformedValue).doesNotContainValue(ORIGINAL_VALUE);

        assertThat(transformedValue).containsKey(OTHER_FIELD_NAME);
        assertThat(transformedValue).containsValue(OTHER_FIELD_VALUE);
    }

    @Test
    public void testSchemalessKeyWithFieldReplace() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.FIELD_CONFIG, FIELD_NAME);
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformKey.configure(config);

        Map<String, Object> key = new HashMap<>();
        key.put(FIELD_NAME, ORIGINAL_VALUE);
        key.put(OTHER_FIELD_NAME, OTHER_FIELD_VALUE);

        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", null, key, null, null));
        assertThat(transformed).isNotNull();
        assertThat(transformed.key()).isNotNull();
        Map transformedKey = (Map) transformed.key();
        assertThat(transformedKey.size()).isEqualTo(2);
        assertThat(transformedKey).containsKey(FIELD_NAME);
        assertThat(transformedKey).containsValue(EXPECTED_FIRST_VALUE);
        assertThat(transformedKey).doesNotContainValue(ORIGINAL_VALUE);

        assertThat(transformedKey).containsKey(OTHER_FIELD_NAME);
        assertThat(transformedKey).containsValue(OTHER_FIELD_VALUE);
    }

    @Test
    public void testSchemalessValueWithFieldReplace() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.FIELD_CONFIG, FIELD_NAME);
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformValue.configure(config);

        Map<String, Object> value = new HashMap<>();
        value.put(FIELD_NAME, ORIGINAL_VALUE);
        value.put(OTHER_FIELD_NAME, OTHER_FIELD_VALUE);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", null, null, value));
        assertThat(transformed).isNotNull();
        assertThat(transformed.value()).isNotNull();
        Map transformedValue = (Map) transformed.value();
        assertThat(transformedValue.size()).isEqualTo(2);
        assertThat(transformedValue).containsKey(FIELD_NAME);
        assertThat(transformedValue).containsValue(EXPECTED_FIRST_VALUE);
        assertThat(transformedValue).doesNotContainValue(ORIGINAL_VALUE);

        assertThat(transformedValue).containsKey(OTHER_FIELD_NAME);
        assertThat(transformedValue).containsValue(OTHER_FIELD_VALUE);
    }

    @Test
    public void testWithSchemaEntireKeyReplace() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformKey.configure(config);

        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", Schema.STRING_SCHEMA, ORIGINAL_VALUE, null, null));
        assertThat(transformed).isNotNull();
        assertThat(transformed.key()).isEqualTo(EXPECTED_FIRST_VALUE);
    }

    @Test
    public void testWithSchemaEntireValueReplace() {
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformValue.configure(config);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, Schema.STRING_SCHEMA, ORIGINAL_VALUE));
        assertThat(transformed).isNotNull();
        assertThat(transformed.value()).isEqualTo(EXPECTED_FIRST_VALUE);
    }

    @Test
    public void testWithSchemaKeyWithFieldReplaceFirst() {
        //prepare config
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.FIELD_CONFIG, FIELD_NAME);
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformKey.configure(config);

        //schema
        Schema structWithStringFieldSchema = SchemaBuilder.struct()
            .field(FIELD_NAME, Schema.STRING_SCHEMA)
            .field(OTHER_FIELD_NAME, Schema.INT64_SCHEMA)
            .build();
        //struct for the schema
        Struct original = new Struct(structWithStringFieldSchema);
        original.put(FIELD_NAME, ORIGINAL_VALUE);
        original.put(OTHER_FIELD_NAME, OTHER_FIELD_VALUE);

        //transform
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", structWithStringFieldSchema, original, null, null));

        //assert
        assertThat(transformed).isNotNull();
        assertThat(transformed.key()).isNotNull();
        Struct transformedKey = (Struct) transformed.key();
        assertThat(transformedKey.schema()).isNotNull();

        String transformedField = transformedKey.getString(FIELD_NAME);
        assertThat(transformedField).isNotNull();
        assertThat(transformedField).isEqualTo(EXPECTED_FIRST_VALUE);

        Long otherField = transformedKey.getInt64(OTHER_FIELD_NAME);
        assertThat(otherField).isNotNull();
        assertThat(otherField).isEqualTo(OTHER_FIELD_VALUE);
    }

    @Test
    public void testSchemalessValueWithFieldReplaceFirst() {
        //prepare config
        Map<String, String> config = new HashMap<>();
        config.put(CharTrimTransformConfig.FIELD_CONFIG, FIELD_NAME);
        config.put(CharTrimTransformConfig.CHARACTER_CONFIG, "\"");
        xformValue.configure(config);

        //schema
        Schema structWithStringFieldSchema = SchemaBuilder.struct()
            .field(FIELD_NAME, Schema.STRING_SCHEMA)
            .field(OTHER_FIELD_NAME, Schema.INT64_SCHEMA)
            .build();
        //struct for the schema
        Struct original = new Struct(structWithStringFieldSchema);
        original.put(FIELD_NAME, ORIGINAL_VALUE);
        original.put(OTHER_FIELD_NAME, OTHER_FIELD_VALUE);

        //transform
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", structWithStringFieldSchema, original));

        //assert
        assertThat(transformed).isNotNull();
        assertThat(transformed.value()).isNotNull();
        Struct transformedValue = (Struct) transformed.value();
        assertThat(transformedValue.schema()).isNotNull();

        String transformedField = transformedValue.getString(FIELD_NAME);
        assertThat(transformedField).isNotNull();
        assertThat(transformedField).isEqualTo(EXPECTED_FIRST_VALUE);

        Long otherField = transformedValue.getInt64(OTHER_FIELD_NAME);
        assertThat(otherField).isNotNull();
        assertThat(otherField).isEqualTo(OTHER_FIELD_VALUE);
    }

}