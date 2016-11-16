package org.ag.hbase.util.admin;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class AdminUtils {
    Connection hbaseConnection ;
    Admin hbaseAdmin ;
    public AdminUtils(Connection hbaseConn) throws IOException {
        this.hbaseConnection = hbaseConn ;
        this.hbaseAdmin = this.hbaseConnection.getAdmin();
    }

    public static void main(String[] args) throws IOException {
        Connection conn = new HBaseConnectionBuilder("localhost:2181").sessionTimeOut(12000).build() ;
        AdminUtils admUtil = new AdminUtils(conn) ;
        admUtil.tables().forEach(x->System.out.println(x));
    }


    public boolean createTable(String nameSpace, String name, List<String> columnFamilies, ColumnFamilyBuilder cgBuilder){
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(nameSpace,name)) ;
        columnFamilies.stream().forEach(x->desc.addFamily(cgBuilder.build(x)));
        try {
            this.hbaseAdmin.createTable(desc);
            return true ;
        } catch (IOException e) {
            return false;
        }
    }

    public Set<String> tables() {
        try{
            return Arrays.stream(this.hbaseAdmin.listTableNames())
                    .map(x -> x.getNameAsString())
                    .collect(Collectors.toSet());
        }
        catch (Exception e){
            return null ;
        }
    }

}
