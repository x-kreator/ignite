/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.UUID;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class JdbcSqlStreamingTest extends AbstractIndexingCommonTest {
    /** */
    private static final int ROW_NUM = 1600;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStreamingWithFunctionValues() throws Exception {
        IgniteEx g0 = startGrid(0);

        Driver driver = new IgniteJdbcThinDriver();
        Connection conn = driver.connect("jdbc:ignite:thin://localhost", new Properties());

        PreparedStatement ps = conn.prepareStatement("CREATE TABLE city1 (id int primary key, name varchar, name1 uuid);");
        ps.execute();
        ps.close();

        ps = conn.prepareStatement("SET STREAMING ON ALLOW_OVERWRITE ON");
        ps.execute();
        ps.close();

        String sql = "INSERT INTO city1 (id, name, name1) VALUES (?, ?, RANDOM_UUID())";
        ps = conn.prepareStatement(sql);
        for (int i = 0; i < ROW_NUM; i++) {
            String s1 = String.valueOf(Math.random());
            ps.setInt(1, i);
            ps.setString(2, s1);
            ps.execute();
        }
        ps.close();

        ps = conn.prepareStatement("SET STREAMING OFF");
        ps.execute();
        ps.close();

        ps = conn.prepareStatement("SELECT id, name, name1 FROM city1 ORDER BY id");
        ResultSet rs = ps.executeQuery();
        int rowNum = 0;
        while (rs.next()) {
            assertEquals(rowNum, rs.getInt(1));
            Double.parseDouble(rs.getString(2));
            UUID.fromString(rs.getString(3));
            rowNum++;
        }
        assertEquals(ROW_NUM, rowNum);
        ps.close();

        conn.close();
    }
}
