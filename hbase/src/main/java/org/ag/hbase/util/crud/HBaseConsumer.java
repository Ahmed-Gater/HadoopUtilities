package org.ag.hbase.util.crud;

import org.ag.hbase.conf.HBaseConnectionBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.javatuples.Quartet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ahmed.gater on 23/11/2016.
 */
public class HBaseConsumer {

    Table table;

    public HBaseConsumer(Connection conn, String tablename) {

        try {
            table = conn.getTable(TableName.valueOf(tablename));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public List<Quartet<String,String,String,String>> getRecords(String key, List<Pair<String,String>> famQuals){
        Get get = new Get(key.getBytes()) ;
        for(Pair<String,String> fq: famQuals){
            get=get.addColumn(fq.getFirst().getBytes(),fq.getSecond().getBytes());
        }

        try {
            return processResults(this.table.get(get)) ;
        }
        catch(Exception e){
            return null ;
        }
    }

    public Quartet<String,String,String,String> getRecord(String key, String family, String qualifier) {
        List<Pair<String,String>> famQuals = new ArrayList<>() ;
        famQuals.add(new Pair<>(family,qualifier));
        try{
            return getRecords(key, famQuals).get(0) ;
        }
        catch (Exception e){
            return null ;
        }
    }

    public Quartet<String,String,String,String> scan(){
        return null ;
    }

    private List<Quartet<String,String,String,String>> processResults(Result result){
        CellScanner scanner = result.cellScanner();
        List<Quartet<String,String,String,String>> qrts = new ArrayList<>() ;
        try {
            while (scanner.advance()) {
                Cell cell = scanner.current();
                qrts.add(new Quartet<>(new String(CellUtil.cloneRow(cell), StandardCharsets.UTF_8),
                        new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8),
                        new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),
                        new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8))) ;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return qrts ;
    }

    public static void main(String[] args) throws Exception {
        HBaseConsumer g = new HBaseConsumer(new HBaseConnectionBuilder("quickstart.cloudera:2181").build(), "t1");
        List<Pair<String,String>> famQuals = new ArrayList<>() ;
        famQuals.add(new Pair<>("cf1", "col1"));
        System.out.println(g.getRecords("key1", famQuals));

    }


}
