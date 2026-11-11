package com.paymentskafka.paymentapi.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.paymentskafka.paymentapi.TestcontainersConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Guards the outbox table against drift from the DDL documented in
 * docs/architecture.md, which Debezium is configured against.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OutboxSchemaTest {

    record Column(String name, String type, Integer maxLength, boolean nullable, String defaultValue) {
    }

    @Autowired
    JdbcClient jdbc;

    @Test
    void outboxColumnsMatchTheDocumentedContract() {
        List<Column> columns = jdbc.sql("""
                        SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'outbox'
                        ORDER BY ordinal_position
                        """)
                .query((rs, n) -> new Column(
                        rs.getString("column_name"),
                        rs.getString("data_type"),
                        (Integer) rs.getObject("character_maximum_length"),
                        "YES".equals(rs.getString("is_nullable")),
                        rs.getString("column_default")))
                .list();

        assertThat(columns).containsExactly(
                new Column("id", "uuid", null, false, null),
                new Column("aggregate_type", "character varying", 64, false, null),
                new Column("aggregate_id", "character varying", 64, false, null),
                new Column("merchant_id", "character varying", 64, false, null),
                new Column("event_type", "character varying", 64, false, null),
                new Column("payload", "jsonb", null, false, null),
                new Column("created_at", "timestamp with time zone", null, false, "now()"));
    }

    @Test
    void outboxPrimaryKeyIsId() {
        List<String> keyColumns = jdbc.sql("""
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = 'public' AND tc.table_name = 'outbox'
                          AND tc.constraint_type = 'PRIMARY KEY'
                        """)
                .query(String.class)
                .list();

        assertThat(keyColumns).containsExactly("id");
    }

    @Test
    void paymentsTableExists() {
        Integer count = jdbc.sql("""
                        SELECT count(*) FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'payments'
                        """)
                .query(Integer.class)
                .single();

        assertThat(count).isEqualTo(1);
    }
}
