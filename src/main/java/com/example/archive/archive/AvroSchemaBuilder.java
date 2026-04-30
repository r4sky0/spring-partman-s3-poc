package com.example.archive.archive;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Maps a JDBC {@link ResultSetMetaData} to an Avro {@link Schema} suitable for
 * an {@code AvroParquetWriter}. The mapping covers the subset of types the
 * {@code events} schema uses today (BIGINT, TEXT, UUID, JSONB, TIMESTAMPTZ);
 * extending it for additional column types is a matter of one more {@code case}.
 *
 * <p>Postgres-specific types (UUID, JSONB) come through JDBC as
 * {@link Types#OTHER} — we discriminate via {@link ResultSetMetaData#getColumnTypeName}.
 *
 * <p>All fields are nullable (union with {@code "null"}) regardless of the SQL
 * NOT NULL constraint, because Avro nullability is a schema-level concern and
 * being permissive here keeps the writer happy if a NOT NULL is ever relaxed.
 */
public final class AvroSchemaBuilder {

    private AvroSchemaBuilder() {}

    public static Schema fromResultSet(String recordName, ResultSetMetaData md) throws SQLException {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(recordName)
                .namespace("com.example.archive.events")
                .fields();

        for (int i = 1; i <= md.getColumnCount(); i++) {
            String name = md.getColumnLabel(i);
            int sqlType = md.getColumnType(i);
            String typeName = md.getColumnTypeName(i);

            Schema fieldSchema = avroTypeFor(sqlType, typeName);
            fields = fields.name(name)
                    .type(Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema))
                    .withDefault(null);
        }
        return fields.endRecord();
    }

    private static Schema avroTypeFor(int sqlType, String typeName) {
        return switch (sqlType) {
            case Types.BIGINT -> Schema.create(Schema.Type.LONG);
            case Types.INTEGER, Types.SMALLINT -> Schema.create(Schema.Type.INT);
            case Types.VARCHAR, Types.LONGVARCHAR, Types.CHAR -> Schema.create(Schema.Type.STRING);
            case Types.BOOLEAN, Types.BIT -> Schema.create(Schema.Type.BOOLEAN);
            case Types.DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case Types.FLOAT, Types.REAL -> Schema.create(Schema.Type.FLOAT);
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                    LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case Types.DATE ->
                    LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            // Postgres uuid + jsonb come through as Types.OTHER; lean on the type name.
            case Types.OTHER -> switch (typeName == null ? "" : typeName.toLowerCase()) {
                case "uuid", "jsonb", "json" -> Schema.create(Schema.Type.STRING);
                default -> Schema.create(Schema.Type.STRING);
            };
            // Fallback: store as string so the writer still produces a valid file. Loses type
            // fidelity, which is the right tradeoff for a column we didn't anticipate.
            default -> Schema.create(Schema.Type.STRING);
        };
    }
}
