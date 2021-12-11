package com.edge.r2dbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
@Component
public class JdbcConnector {

    private static String SELECT_STATEMENT;
    private static String UPDATE_STATEMENT;

    @Autowired
    JdbcConfig jdbcConfig;

    @PostConstruct
    private void init() {
        setSqlStatements();
    }

    public DataSource getDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(jdbcConfig.getDriverClass());
        dataSource.setUrl(jdbcConfig.getUrl());
        dataSource.setUsername(jdbcConfig.getUsername());
        dataSource.setPassword(jdbcConfig.getPassword());
        dataSource.setValidationQuery(jdbcConfig.getValidationQuery());
        dataSource.setMaxIdle(jdbcConfig.getMaxIdle());
        dataSource.setMaxTotal(jdbcConfig.getMaxConnections());
        return dataSource;
    }

    public void setSqlStatements() {
        SELECT_STATEMENT = "SELECT sender_seqnum, target_seqnum FROM sessions";
        UPDATE_STATEMENT = "UPDATE sessions SET sender_seqnum=?, target_seqnum=?";
    }

    public PreparedStatement executeUpdateSeqNumStatement(Connection conn, SeqNum seqNum) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(UPDATE_STATEMENT);
        stmt.setString(1, String.valueOf(seqNum.getSenderSeqNum()));
        stmt.setString(2, String.valueOf(seqNum.getTargetSeqNum()));
        return stmt;
    }

    public SeqNum getSeqNum() throws SQLException {
        Connection conn = null;
        try {
            conn = getDataSource().getConnection();
            PreparedStatement stmt = conn.prepareStatement(SELECT_STATEMENT);
            ResultSet resultSet = stmt.executeQuery();
            SeqNum seqNum = SeqNum.builder().build();
            if (resultSet.next()) {
                seqNum.setSenderSeqNum(resultSet.getInt(1));
                seqNum.setTargetSeqNum(resultSet.getInt(2));
            }
            return seqNum;
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    public boolean updateSeqNum(SeqNum seqNum) throws SQLException {
        Connection conn = null;
        try {
            conn = getDataSource().getConnection();
            PreparedStatement stmt = executeUpdateSeqNumStatement(conn, seqNum);
            boolean status = stmt.execute();
            return !status && stmt.getUpdateCount() > 0;
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}
