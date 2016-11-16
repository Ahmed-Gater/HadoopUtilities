package org.ag.hbase.util.admin;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class AdminUtilsTest {
    @Test
    public void createTable() throws Exception {
        Connection conn = new HBaseConnectionBuilder("localhost:2181").sessionTimeOut(12000).build() ;
        ColumnFamilyBuilder h = new ColumnFamilyBuilder() ;
        AdminUtils admUtil = new AdminUtils(conn) ;
        admUtil.createTable("default","test2", ImmutableList.of("cf1", "cf2"),h) ;
        Assert.assertTrue(admUtil.tables().contains("test2"));
    }

}