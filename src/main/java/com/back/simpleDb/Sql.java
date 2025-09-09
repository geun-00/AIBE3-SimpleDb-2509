package com.back.simpleDb;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class Sql {

    private final StringBuilder sqlBuilder = new StringBuilder();
    private final Connection connection;
    private final boolean devMode;

    public Sql(Connection connection, boolean devMode) {
        this.connection = connection;
        this.devMode = devMode;
    }

    public Sql append(String sql) {
        sqlBuilder.append(sql).append("\n");
        return this;
    }

    public Sql append(String sql, Object... params) {
        int paramIndex = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '?' && paramIndex < params.length) {
                Object param = params[paramIndex++];
                sqlBuilder.append("'").append(param.toString()).append("'");
            } else {
                sqlBuilder.append(c);
            }
        }

        sqlBuilder.append("\n");
        return this;
    }

    public Sql appendIn(String sql, Object... params) {
        String inParams = Arrays.stream(params)
                                .map(obj -> "'" + obj.toString() + "'")
                                .collect(Collectors.joining(", "));
        sqlBuilder.append(sql.replaceFirst("\\?", inParams))
                  .append("\n");
        return this;
    }

    public long insert() {
        String sql = createSql();
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

    private void logQuery(String sql) {
        if (devMode) {
            log.info("executed query : {}", sql);
        }
    }

    public int update() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql)) {
            return pstm.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    public int delete() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql)) {
            return pstm.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> result = new ArrayList<>();

        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    map.put(columnName, rs.getObject(i));
                }

                result.add(map);
            }

            return result;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    public Map<String, Object> selectRow() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            Map<String, Object> map = new HashMap<>();

            if (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    map.put(columnName, rs.getObject(i));
                }
            }

            return map;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    public <T> T selectRow(Class<T> cls) {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            if (rs.next()) {
                T instance = cls.getConstructor().newInstance();

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);

                    Field field = cls.getDeclaredField(columnName);
                    field.setAccessible(true);
                    field.set(instance, rs.getObject(i));
                }

                return instance;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }

        return null;
    }

    public <T> List<T> selectRows(Class<T> cls) {
        String sql = createSql();
        List<T> result = new ArrayList<>();

        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            while (rs.next()) {
                T instance = cls.getConstructor().newInstance();

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);

                    Field field = cls.getDeclaredField(columnName);
                    field.setAccessible(true);
                    field.set(instance, rs.getObject(i));
                }

                result.add(instance);
            }

            return result;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    public long selectLong() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }

        return -1;
    }

    public LocalDateTime selectDatetime() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            if (rs.next()) {
                return rs.getTimestamp(1).toLocalDateTime();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }

        return null;
    }

    public String selectString() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            if (rs.next()) {
                return rs.getString(1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }

        return null;
    }

    public Boolean selectBoolean() {
        String sql = createSql();
        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }

        return null;
    }

    public List<Long> selectLongs() {
        String sql = createSql();
        List<Long> result = new ArrayList<>();

        try (PreparedStatement pstm = connection.prepareStatement(sql);
             ResultSet rs = pstm.executeQuery()) {

            while (rs.next()) {
                result.add(rs.getLong(1));
            }

            return result;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            logQuery(sql);
        }
    }

    private String createSql() {
        return sqlBuilder.toString();
    }
}
