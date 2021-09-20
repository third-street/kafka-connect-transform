package com.thirdstreet.transform;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CharTrimTransformConfig extends AbstractConfig {

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";

    public static final String CHARACTER_CONFIG = "character";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(FIELD_CONFIG,
                ConfigDef.Type.STRING,
                FIELD_DEFAULT,
                ConfigDef.Importance.HIGH,
                "The field containing the string to apply replacement, or empty if the entire value is a string.")
        .define(CHARACTER_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Character to use for trimming.");

    public static final String REPLACEMENT_METHOD_ALL = "ALL";
    public static final String REPLACEMENT_METHOD_FIRST = "FIRST";

    public static final Set<String> VALID_REPLACEMENT_METHODS = new HashSet<>(Arrays.asList(REPLACEMENT_METHOD_ALL, REPLACEMENT_METHOD_FIRST));

    private final String field;
    private final String character;

    public CharTrimTransformConfig(final Map<?, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.field = getString(FIELD_CONFIG);
        this.character = getString(CHARACTER_CONFIG);

        if (character.trim().isEmpty() || character.length() != 1) {
            throw new ConfigException("CharTrimTransform requires regex option to be specified");
        }
    }

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    public String getField() {
        return field;
    }

    public String getCharacter() {
        return character;
    }
}
