package com.paymentskafka.paymentapi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class PaymentApiApplicationTest {

    @Autowired
    Environment environment;

    @Autowired
    JdbcClient jdbc;

    @Test
    void contextLoadsAgainstPostgres() {
        Integer one = jdbc.sql("SELECT 1").query(Integer.class).single();
        assertThat(one).isEqualTo(1);
    }

    @Test
    void defaultsMatchTheArchitectureDocument() {
        assertThat(environment.getProperty("server.port")).isEqualTo("8081");
    }
}
