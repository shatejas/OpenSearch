/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.mapper.ParametrizedFieldMapper.Parameter.stringParam;

@PublicApi(since = "3.1.0")
public class NamespaceFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "namespace";

    public static final Mapper.TypeParser PARSER = new TypeParser();

    private static class TypeParser implements Mapper.TypeParser {

        public TypeParser() {}

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context) throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, context, node);
            return builder;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context, ObjectMapper.Builder objBuilder)
            throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, context, node);
            return builder;
        }
    }

    @PublicApi(since = "3.1.0")
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private static NamespaceFieldMapper toType(FieldMapper in) {
            return (NamespaceFieldMapper) in;
        }

        private Parameter<String> namespaceField = stringParam("field",
            false,
            m -> toType(m).namespaceField,
            null)
            .acceptsNull();

        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
        }

        protected Builder(String name, String namespaceField) {
            super(name);
            this.namespaceField.setValue(namespaceField);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(namespaceField);
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            NamespaceFieldType namespaceFieldType = new NamespaceFieldType(CONTENT_TYPE);
            namespaceFieldType.setFieldName(this.namespaceField.getValue());

            return new NamespaceFieldMapper(name, namespaceFieldType, this);
        }
    }

    private final String namespaceField;

    /**
     * Creates a new ParametrizedFieldMapper
     *
     * @param simpleName
     * @param type
     */
    protected NamespaceFieldMapper(String simpleName, NamespaceFieldType type, Builder builder) {
        super(simpleName, type, MultiFields.empty(), CopyTo.empty());
        this.namespaceField = builder.namespaceField.getValue();
    }

    @Override
    public Builder getMergeBuilder() {
        return new Builder(simpleName(), namespaceField);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        getMergeBuilder().toXContent(builder, includeDefaults);
        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public NamespaceFieldType fieldType() {
        return (NamespaceFieldType) super.fieldType();
    }
}
