package com.paymentskafka.paymentapi.payment;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {

    private static final String SELECT_BY_ID = """
            SELECT id, merchant_id, amount_cents, currency, status, sequence, created_at, updated_at
            FROM payments WHERE id = :id""";

    private final JdbcClient jdbc;

    public PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Payment> findById(UUID id) {
        return jdbc.sql(SELECT_BY_ID).param("id", id).query(PaymentRepository::map).optional();
    }

    /**
     * Reads the payment and locks its row until the surrounding transaction ends,
     * so concurrent transitions on the same payment are serialized.
     */
    public Optional<Payment> findByIdForUpdate(UUID id) {
        return jdbc.sql(SELECT_BY_ID + " FOR UPDATE").param("id", id).query(PaymentRepository::map).optional();
    }

    public void insert(Payment payment) {
        jdbc.sql("""
                        INSERT INTO payments (id, merchant_id, amount_cents, currency, status, sequence, created_at, updated_at)
                        VALUES (:id, :merchantId, :amountCents, :currency, :status, :sequence, :createdAt, :updatedAt)
                        """)
                .param("id", payment.id())
                .param("merchantId", payment.merchantId())
                .param("amountCents", payment.amountCents())
                .param("currency", payment.currency())
                .param("status", payment.status().name())
                .param("sequence", payment.sequence())
                .param("createdAt", Timestamp.from(payment.createdAt()))
                .param("updatedAt", Timestamp.from(payment.updatedAt()))
                .update();
    }

    public void update(Payment payment) {
        jdbc.sql("UPDATE payments SET status = :status, sequence = :sequence, updated_at = :updatedAt WHERE id = :id")
                .param("id", payment.id())
                .param("status", payment.status().name())
                .param("sequence", payment.sequence())
                .param("updatedAt", Timestamp.from(payment.updatedAt()))
                .update();
    }

    private static Payment map(ResultSet rs, int rowNum) throws SQLException {
        return new Payment(
                rs.getObject("id", UUID.class),
                rs.getString("merchant_id"),
                rs.getLong("amount_cents"),
                rs.getString("currency"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getInt("sequence"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }
}
