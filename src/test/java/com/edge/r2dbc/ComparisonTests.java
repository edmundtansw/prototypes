package com.edge.r2dbc;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest(classes = {R2dbcConnector.class, JdbcConnector.class})
@EnableConfigurationProperties(value = JdbcConfig.class)
@ActiveProfiles("test")
class ComparisonTests {

    final static int MAX_UPDATE_RUNS = 140;

    @Autowired
    R2dbcConnector r2dbcConnector;

    @Autowired
    JdbcConnector jdbcConnector;

    @BeforeEach
    private void resetSeqNum() {
        r2dbcConnector.updateSeqNum(SeqNum.builder()
                .senderSeqNum(1)
                .targetSeqNum(1)
                .build()).subscribe();
    }

    private void r2dbcAssertEqualSeqNum(SeqNum seqNum) {
        Mono<SeqNum> result = r2dbcConnector.getSeqNum().log();
        StepVerifier.create(result)
                .expectNext(seqNum)
                .verifyComplete();
    }

    private void jdbcAssertEqualSeqNum(SeqNum seqNum) throws SQLException {
        SeqNum result = jdbcConnector.getSeqNum();
        assertEquals(seqNum.getSenderSeqNum(), result.getSenderSeqNum());
        assertEquals(seqNum.getTargetSeqNum(), result.getTargetSeqNum());
    }

    @Test
    void r2dbc_expectDefaultSeqNum() {
        SeqNum defaultSeqNum = SeqNum.builder()
                .senderSeqNum(1)
                .targetSeqNum(1)
                .build();
        r2dbcAssertEqualSeqNum(defaultSeqNum);
    }

    @Test
    void jdbc_expectDefaultSeqNum() throws SQLException {
        SeqNum defaultSeqNum = SeqNum.builder()
                .senderSeqNum(1)
                .targetSeqNum(1)
                .build();
        jdbcAssertEqualSeqNum(defaultSeqNum);
    }

    @Test
    void jdbc_updateSeqNum() throws SQLException {
        for (int i = 0; i < 30; i++) { //Warm-up
            resetSeqNum();
        }

        final int numOfRuns = MAX_UPDATE_RUNS;
        SeqNum seqNum = SeqNum.builder().build();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i <= numOfRuns; i++) {
            seqNum.setSenderSeqNum(i);
            seqNum.setTargetSeqNum(i);
            jdbcConnector.updateSeqNum(seqNum);
        }

        jdbcAssertEqualSeqNum(seqNum);
        log.info("Total Latency = {}", (System.currentTimeMillis() - timestamp));
        log.info("Avg Latency = {}", (System.currentTimeMillis() - timestamp) / numOfRuns);
    }

    @Test
    void r2dbc_updateSeqNum() {
        for (int i = 0; i < 30; i++) { //Warm-up
            resetSeqNum();
        }

        final int numOfRuns = MAX_UPDATE_RUNS;
        SeqNum seqNum = SeqNum.builder().build();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i <= numOfRuns; i++) {
            seqNum.setSenderSeqNum(i);
            seqNum.setTargetSeqNum(i);
            r2dbcConnector.updateSeqNum(seqNum)
                    .subscribe();
        }

        r2dbcAssertEqualSeqNum(seqNum);
        log.info("Total Latency = {}", (System.currentTimeMillis() - timestamp));
        log.info("Avg Latency = {}", (System.currentTimeMillis() - timestamp) / numOfRuns);
    }
}
