package com.edge.r2dbc;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class R2dbcConnector {

    private MariadbConnectionFactory connFactory;
    @Getter
    private static ConnectionPool pool;
    @Getter
    private Connection conn;
    private static String SELECT_STATEMENT;
    private static String UPDATE_STATEMENT;

    @PostConstruct
    private void init() {
        setSqlStatements();
        createConnectionPool();
    }

    public void setSqlStatements() {
        SELECT_STATEMENT = "SELECT sender_seqnum, target_seqnum FROM sessions";
        UPDATE_STATEMENT = "UPDATE sessions SET sender_seqnum=?, target_seqnum=?";
    }

    public void createConnectionPool() {
        try {
            // Configure and Create Connection Factory
            MariadbConnectionConfiguration factoryConfig = MariadbConnectionConfiguration
                    .builder()
                    .host("127.0.0.1")
                    .port(3306)
                    .username("root")
                    .password("")
                    .database("edge")
                    .build();

            this.connFactory = new MariadbConnectionFactory(factoryConfig);

            // Configure Connection Pool
            ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration
                    .builder(connFactory)
                    .maxSize(3)
                    .build();

            this.pool = new ConnectionPool(poolConfig);
        }
        // Catch Exception
        catch (IllegalArgumentException e) {
            log.error("Issue creating connection pool", e);
        }
    }

    public Statement executeUpdateSeqNumStatement(Connection conn, SeqNum seqNum) {
        Statement stmt = conn.createStatement(UPDATE_STATEMENT);
        stmt.bind(0, seqNum.getSenderSeqNum());
        stmt.bind(1, seqNum.getTargetSeqNum());
        return stmt;
    }

    public Mono<SeqNum> getSeqNum() {
        return Flux.usingWhen(
                pool.create(),
                conn -> conn.createStatement(SELECT_STATEMENT).execute(),
                conn -> conn.close()
        ).flatMap(result -> result.map((row, meta) ->
                        SeqNum.builder()
                                .senderSeqNum(row.get(0, Integer.class))
                                .targetSeqNum(row.get(1, Integer.class))
                                .build()
        )).single();
    }

    public Flux updateSeqNum(SeqNum seqNum) {
        return Flux.usingWhen(
                pool.create(),
                conn -> executeUpdateSeqNumStatement(conn, seqNum).execute(),
                conn -> conn.close()
        );
    }
}
