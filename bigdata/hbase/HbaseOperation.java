package com.jike.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HbaseOperation {
    static Configuration conf = null;
    static Connection conn = null;
    static Admin admin = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","47.101.206.249:2181,47.101.216.12:2181,47.101.204.23:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropTable() throws IOException {
        TableName tableName = TableName.valueOf("minggao:student");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName+" 删除成功");
    }

    @Test
    public void createTable() throws IOException {

        try {
            //获取命名空间看是否存在,不存在会直接抛 NamespaceNotFoundException 异常(可以查看底层代码实现)  使用 try catch 捕捉
            NamespaceDescriptor rel = admin.getNamespaceDescriptor("minggao");
        } catch (NamespaceNotFoundException  e) {
            // 不存在创建一个命名空间
            NamespaceDescriptor namespace = NamespaceDescriptor.create("minggao").build();
            admin.createNamespace(namespace);
        }

        //HTableDescriptor hd = new HTableDescriptor(TableName.valueOf("minggao:student"));
        TableDescriptorBuilder td = TableDescriptorBuilder.newBuilder(TableName.valueOf("minggao:student"));
        //HColumnDescriptor column1 = new HColumnDescriptor("info");
        //HColumnDescriptor column2 = new HColumnDescriptor("score");
        ColumnFamilyDescriptor column1 = ColumnFamilyDescriptorBuilder.of("name");
        ColumnFamilyDescriptor column2 = ColumnFamilyDescriptorBuilder.of("info");
        ColumnFamilyDescriptor column3 = ColumnFamilyDescriptorBuilder.of("score");
        td.setColumnFamily(column1);
        td.setColumnFamily(column2);
        td.setColumnFamily(column3);
        TableDescriptor build = td.build();
        admin.createTable(build);
        admin.close();
        conn.close();

    }

    @Test
    public void putTable() throws IOException {
        Table table = conn.getTable(TableName.valueOf("minggao:student"));
//        Put put = new Put(Bytes.toBytes("rowid"+1));
//        put.addColumn(Bytes.toBytes("name"),
//                Bytes.toBytes(""),
//                Bytes.toBytes("Tom"));
//        put.addColumn(Bytes.toBytes("info"),
//                Bytes.toBytes("student_id"),
//                Bytes.toBytes("20210000000001"));
//        put.addColumn(Bytes.toBytes("info"),
//                Bytes.toBytes("class"),
//                Bytes.toBytes("1"));
//        put.addColumn(Bytes.toBytes("score"),
//                Bytes.toBytes("understanding"),
//                Bytes.toBytes("75"));
//        put.addColumn(Bytes.toBytes("score"),
//                Bytes.toBytes("programming"),
//                Bytes.toBytes("82"));
          Put put = new Put(Bytes.toBytes("rowid"+2));
        put.addColumn(Bytes.toBytes("name"),
                Bytes.toBytes(""),
                Bytes.toBytes("minggao"));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("student_id"),
                Bytes.toBytes("20210760010323"));
        put.addColumn(Bytes.toBytes("info"),
                Bytes.toBytes("class"),
                Bytes.toBytes("4"));
        put.addColumn(Bytes.toBytes("score"),
                Bytes.toBytes("understanding"),
                Bytes.toBytes("80"));
        put.addColumn(Bytes.toBytes("score"),
                Bytes.toBytes("programming"),
                Bytes.toBytes("80"));
        table.put(put);
        System.out.println("插入数据成功");
    }

    @Test
    public void delData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("minggao:student"));
        Delete delete = new Delete(Bytes.toBytes(1));
        table.delete(delete);
        table.close();
    }

    @Test
    public void scanData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("minggao:student"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result:scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String fName = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowkey:"+rowKey+",fname:"+fName+",qualifier:"+qualifier
                +",value:"+value+",timestamp:"+timestamp);
            }
        }
    }

    @Test
    public void getData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("minggao:student"));
        Get get = new Get(Bytes.toBytes("rowid"+2));
        Result result = table.get(get);
        List<Cell> cells2 = result.listCells();
        for (Cell cell : cells2) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            long timestamp = cell.getTimestamp();
            String fName = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("rowkey:"+rowKey+",fname:"+fName+",qualifier:"+qualifier
                    +",value:"+value+",timestamp:"+timestamp);
        }
    }
}
