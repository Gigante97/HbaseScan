import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.FileWriter;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;


import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;

import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Table;


import static org.apache.hadoop.hbase.util.Bytes.toBytes;


public class ScanTable{

    private static Configuration conf;
    private static Connection connection;
    private static TableName tableName;
    private static byte[] family = Bytes.toBytes("d");
    private static byte[] column = Bytes.toBytes("did");



    public static void main(String args[]) throws IOException{


        conf = HBaseConfiguration.create();
        conf.setStrings("hbase.zookeeper.quorum", "hbase01.test01.common.crpt.tech");

        connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin.available(conf);
        tableName = TableName.valueOf("FURS_UQA02.TBL_JTI_TRACE_DOCUMENT_ERRORS");

        Table table = connection.getTable(tableName);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                family,
                column,
                CompareOperator.EQUAL,
                Bytes.toBytes("485ff1ee-cb50-479b-89d9-a0a7e76a492d")
        );
        Scan scan = new Scan();
        scan.setLimit(1);
        scan.setFilter(filter);
        //scan.addColumn(family,column);
        scan.addFamily(family);
        ResultScanner resultScanner = table.getScanner(scan);
        Result result = resultScanner.next();
        System.out.println(Bytes.toString(result.getValue(family,column)));

        System.out.println(result.getRow());
        resultScanner.close();








    }
}