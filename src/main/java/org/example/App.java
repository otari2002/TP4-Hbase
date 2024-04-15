package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class App {
    public static final String TABLE_NAME = "students";
    public static final String CF_INFO = "info";
    public static final String CF_GRADES = "grades";
    public static void display(Result result) {
        for(Cell cell: result.rawCells()) {
            byte[] cf = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);

            String cfString = Bytes.toString(cf);
            String qualifierString = Bytes.toString(qualifier);
            String valueString = Bytes.toString(value);

            System.out.println("Column Family: " + cfString +
                    ", Column: " + qualifierString +
                    ", Value: " + valueString);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zookeeper");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "hbase-master:16000");
        // 1
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {

            Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_INFO));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_GRADES));

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

            if (!admin.tableExists(tableName)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created !");
            } else {
                System.err.println("Already exist !");
            }

            Table table = connection.getTable(tableName);

            Put putStudent1 = new Put(Bytes.toBytes("student1"));
            putStudent1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
            putStudent1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("20"));
            putStudent1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("B"));
            putStudent1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(putStudent1);
            System.out.println("Student 1 added !");

            Put putStudent2 = new Put(Bytes.toBytes("student2"));
            putStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
            putStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("22"));
            putStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A"));
            putStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(putStudent2);
            System.out.println("Student 2 added !");

            Get getStudent1 = new Get(Bytes.toBytes("student1"));
            Result resultStudent1 = table.get(getStudent1);
            System.out.println("Information for Student 1:");
            display(resultStudent1);

            Put putUpdateStudent2 = new Put(Bytes.toBytes("student2"));
            putUpdateStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("23"));
            putUpdateStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A+"));
            table.put(putUpdateStudent2);
            System.out.println("Information for Student 2 updated !");

            Delete deleteStudent1 = new Delete(Bytes.toBytes("student1"));
            table.delete(deleteStudent1);
            System.out.println("Student 1 deleted !");

            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("All Students Information:");
            for (Result result : scanner) {
                display(result);
            }
            scanner.close();

            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("The table has been deleted !");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}