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
    public void createTableTest() throws Exception {
        Connection conn = new HBaseConnectionBuilder("localhost:2181").sessionTimeOut(12000).build() ;
        AdminUtils admUtil = new AdminUtils(conn) ;
        admUtil.createTable("default","test2", ImmutableList.of("cf1", "cf2"),new ColumnFamilyBuilder()) ;
        Assert.assertTrue(admUtil.tables().contains("test2"));
        admUtil.deleteTable("default","test2") ;
    }


    @Test
    public void deleteTableTest() throws Exception {
        Connection conn = new HBaseConnectionBuilder("localhost:2181").sessionTimeOut(12000).build() ;
        AdminUtils admUtil = new AdminUtils(conn) ;
        admUtil.createTable("default","test2", ImmutableList.of("cf1", "cf2"),new ColumnFamilyBuilder()) ;
        admUtil.deleteTable("default","test2") ;
        Assert.assertFalse(admUtil.tables().contains("test2"));
    }

}