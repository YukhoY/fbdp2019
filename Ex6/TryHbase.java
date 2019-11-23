package fbdp.hbase;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class TryHbase {
    private static Configuration conf = null;
    private static Connection conn;
    private static Admin admin;
    static {
        conf = HBaseConfiguration.create();
        conf.addResource(Resources.getResource("hbase-site.xml"));
        try{
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tableName, String[] families) throws Exception{
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table already exists!");
            return;
        }
        TableDescriptorBuilder t = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String family : families) {
            t.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
            System.out.println("Column " + family + " added!");
        }

        admin.createTable(t.build());
        System.out.println("Table "+tableName+ " added!");

    }

    public static void dropTable(String tableName) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "does not exist");
            return;
        }

        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            System.out.println("Disabled " + tableName + "!");
        }
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("Deleted " + tableName + "!");
    }

    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table =conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("Inserted" + rowKey +" " + column + " " + value + " to table " + tableName + ".");
    }

    public static void queryTable(String tableName) throws IOException {
        System.out.println("--------------------The Table Starts Here--------------------");

        Table table = conn.getTable(TableName.valueOf(tableName));

        ResultScanner scanner = table.getScanner(new Scan());

        for (Result r:scanner){
            System.out.println("Row key is:"+new String(r.getRow()));

            for (Cell kv:r.rawCells()){
                System.out.println("Family: "+new String(CellUtil.cloneFamily(kv),"utf-8")
                        +"  Value: "+new String(CellUtil.cloneValue(kv),"utf-8")
                        +"  Qualifer: "+new String(CellUtil.cloneQualifier(kv),"utf-8")
                        +"  Timestamp: "+kv.getTimestamp());
            }
        }
        System.out.println("---------------The Table Ends Here----------");
    }

    public static void main(String[] args) throws Exception {
        String  families[]={"Description","Courses","Home"};
        createTable("students", families);
        insert("students","001","Description","Name","Li Lei");
        insert("students","001","Description","Height","176");
        insert("students","001","Courses","Chinese","80");
        insert("students","001","Courses","Math","90");
        insert("students","001","Courses","Physics","95");
        insert("students","001","Home","Province","Zhejiang");

        insert("students","002","Description","Name","Han Meimei");
        insert("students","002","Description","Height","183");
        insert("students","002","Courses","Chinese","88");
        insert("students","002","Courses","Math","77");
        insert("students","002","Courses","Physics","66");
        insert("students","002","Home","Province","Beijing");

        insert("students","003","Description","Name","Xiao Ming");
        insert("students","003","Description","Height","162");
        insert("students","003","Courses","Chinese","90");
        insert("students","003","Courses","Math","90");
        insert("students","003","Courses","Physics","90");
        insert("students","003","Home","Province","Shanghai");
        queryTable("students");
        dropTable("students");
        System.out.println("Ex6 Task finished!");
    }
}
