package com.back.simpleDb;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SqlBuilder {

    private final StringBuilder sqlBuilder = new StringBuilder();

    public void append(CharSequence sql) {
        sqlBuilder.append(sql).append("\n");
    }

    public void append(String sql, Object... params) {
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
    }

    public void appendIn(String sql, Object... params) {
        String inParams = Arrays.stream(params)
                                .map(obj -> "'" + obj.toString() + "'")
                                .collect(Collectors.joining(", "));
        sqlBuilder.append(sql.replaceFirst("\\?", inParams))
                  .append("\n");
    }

    public String getSql() {
        return sqlBuilder.toString();
    }
}
