package com.back.simpleDb;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class MyJdbcTemplate {

    private final Connection connection;
    private final boolean devMode;

    public MyJdbcTemplate(Connection connection, boolean devMode) {
        this.connection = connection;
        this.devMode = devMode;
    }

    public int executeUpdate(String sql) {
        try (PreparedStatement pstm = connection.prepareStatement(sql)) {
            return pstm.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    public int executeInsert(String sql) {
        try (PreparedStatement pstm = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            pstm.executeUpdate();
            ResultSet rs = pstm.getGeneratedKeys();

            if (rs.next()) {
                return rs.getInt(1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }

        return -1;
    }

    public <T> T query(String sql, ResultSetExtractor<T> rse) {
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            return rse.extractData(rs);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    private void logQuery(String sql) {
        if (devMode) {
            log.info("executed query : \n{}", sql);
        }
    }
}
