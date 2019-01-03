/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.coecoyun.dao;

import org.apache.commons.dbcp.BasicDataSource;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.KeyHolder;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class JdbcUtils {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    private NamedParameterJdbcTemplate namedjdbctemp;

    private String url = "";
    private String user = "";
    private String password = "";
    public JdbcUtils(String url) {
        this.url = url;
    }
    public JdbcUtils(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        namedjdbctemp = new NamedParameterJdbcTemplate(getDataSource());
    }

    private DataSource getDataSource() {

        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setUsername(this.user);
        dataSource.setPassword(this.password);
        dataSource.setUrl(this.url);

        dataSource.setMaxActive(10);
        dataSource.setMaxIdle(5);
        dataSource.setValidationQuery("SELECT 1");

        return dataSource;

    }

    public int update(String sql, Map<String, Object> paramMap) {
        return namedjdbctemp.update(sql, paramMap);
    }

    /**
     * @param sql
     * @param paramMap
     * @param generatedKeyHolder
     * @return if insert data return generated keys;update data,return id
     */

    public int update(String sql, Map<String, Object> paramMap, KeyHolder generatedKeyHolder) {
        return namedjdbctemp.update(sql, new MapSqlParameterSource(paramMap), generatedKeyHolder);
    }

    public <T> List<T> query(String sql, Map<String, Object> paramMap, ResultSetExtractor<List<T>> rse) {
        return namedjdbctemp.query(sql, paramMap, rse);
    }

    public <T> T get(String sql, Map<String, Object> paramMap, ResultSetExtractor<T> rse) {
        return namedjdbctemp.query(sql, paramMap, rse);
    }

    public JSONArray getJsonArray(String sql, Map<String, Object> paramMap, ResultSetExtractor<JSONArray> rse) {
        return namedjdbctemp.query(sql, paramMap, rse);
    }
}