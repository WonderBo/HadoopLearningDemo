package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description HBase操作类（HBase的Java API都是完全面向对象实现的）
 */
public class HBaseUtil {

    // HBase连接对象
    private Connection connection = null;
    // HBase配制文件对象
    private Configuration configuration = null;

    /**
     * @description 初始化HBase连接
     */
    public HBaseUtil(String zksStr) {
        configuration = HBaseConfiguration.create();
        /* 只需指定zookeeper集群即可，因为在zookeeper中存贮所有Region的寻址入口,而客户端只需要知道存储的具体位置后，
        即可请求对应的RegionServer并执行相关数据操作。或者引入hbase-site.xml文件也可。*/
        configuration.set("hbase.zookeeper.quorum", zksStr);

        try {
            /* 申请一个Connection可以解决HTable存在的线程不安全问题；同时通过维护固定数量的HTable对象，
        能够在程序运行期间复用这些HTable资源对象。*/
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param tableName 表名
     * @param familyColumns 列族数组
     * @description 创建HBase表
     */
    public void createTable(String tableName, String[] familyColumns) {
        TableName hTableName = TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);
        try {
            // Admin用于管理表结构，类似于DDL
            Admin admin = connection.getAdmin();
            if(admin.tableExists(hTableName)) {
                System.out.println("需要创建的表" + tableName + "已经存在！");
            } else {
                for(String familyColumn : familyColumns) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyColumn);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
                System.err.println("创建表" + tableName + "成功!");
            }
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param tableName 表名
     * @description 删除HBase表
     */
    public void dropTable(String tableName) {
        TableName hTableName = TableName.valueOf(tableName);
        try {
            Admin admin = connection.getAdmin();
            if(admin.tableExists(hTableName)) {
                admin.disableTable(hTableName);
                admin.deleteTable(hTableName);
                System.err.println("删除表" + tableName + "成功!");
            } else {
                System.err.println("需要删除的表" + tableName + "不存在！");
            }
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @param qualifier 字段
     * @param value 值
     * @return
     * @description 向指定HBase表插入一个Cell，用于插入和更新数据
     *
     */
    public boolean insertCell(String tableName, String rowKey, String familyColumn, String qualifier, String value) {
        TableName hTableName = TableName.valueOf(tableName);
        try {
            // Table用于管理表数据，类似于DML（Table的put，delete，get方法均支持传入List进行批量操作）
            Table table = connection.getTable(hTableName);
            // Put、Delete、Get由行键确定，即Put等对应HBase表的一行
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @param qualifiers 字段数组
     * @param values 值数组
     * @return
     * @description 向指定HBase表的固定行固定列族插入多个Cell
     */
    public boolean insertCells(String tableName, String rowKey, String familyColumn, String[] qualifiers, String[] values) {
        TableName hTableName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(hTableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            for(int i = 0; i < qualifiers.length; i++) {
                put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifiers[i]), Bytes.toBytes(values[i]));
            }
            table.put(put);
            table.close();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @param qualifier 字段
     * @return
     * @description 在指定HBase表中删除一个Cell
     */
    public boolean deleteCell(String tableName, String rowKey, String familyColumn, String qualifier) {
        TableName hTableName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(hTableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            if(qualifier != null) {
                // 删除一个Cell
                delete.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifier));
            } else if(familyColumn != null) {
                // 删除列族下的所有Cell
                delete.addFamily(Bytes.toBytes(familyColumn));
            }
            // 未设置qualifier和familyColumn将删除整行
            table.delete(delete);
            table.close();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @return
     * @description 在指定HBase表中删除一行的某个列族
     */
    public boolean deleteFamilyColumn(String tableName, String rowKey, String familyColumn) {
        return deleteCell(tableName, rowKey, familyColumn, null);
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @return
     * @description 在指定HBase表中删除一行
     */
    public boolean deleteRow(String tableName, String rowKey) {
        return deleteCell(tableName, rowKey, null, null);
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @param qualifier 字段
     * @return Get返回Result或者List<Cell>，Scan返回ResultScanner或者List<Result>
     * @description 获取Cell列表
     */
    public List<Cell> getCellList(String tableName, String rowKey, String familyColumn, String qualifier) {
        TableName hTableName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(hTableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            if(qualifier != null) {
                // 获取一个Cell
                get.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(qualifier));
            } else if(familyColumn != null) {
                // 获取列族下的所有Cell
                get.addFamily(Bytes.toBytes(familyColumn));
            }
            // 未设置qualifier和familyColumn将获取整行
            // Result对应一行的查询内容
            Result result = table.get(get);

            List<Cell> cellList = result.listCells();
            table.close();

            return cellList;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @param qualifier 字段
     * @return
     * @description 获取特定Cell的值
     */
    public String getCellValue(String tableName, String rowKey, String familyColumn, String qualifier) {
        List<Cell> cellList = getCellList(tableName, rowKey, familyColumn, qualifier);

        if(cellList == null || cellList.size() == 0) {
            return null;
        }

        return Bytes.toString(CellUtil.cloneValue(cellList.get(0)));
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param familyColumn 列族
     * @return
     * @description 获取特定列族的值
     */
    public Map<String, String> getFamilyColumnValue(String tableName, String rowKey, String familyColumn) {
        Map<String, String> cellMap = new HashMap<String, String>();
        List<Cell> cellList = getCellList(tableName, rowKey, familyColumn, null);

        if(cellList == null || cellList.size() == 0) {
            return null;
        }

        for(Cell cell : cellList) {
            cellMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
        }

        return cellMap;
    }

    /**
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @return
     * @description 获取特定行的值
     */
    public Map<String, Map<String, String>> getRowValue(String tableName, String rowKey) {
        Map<String, Map<String, String>> cellMap = new HashMap<String, Map<String, String>>();
        List<Cell> cellList = getCellList(tableName, rowKey, null, null);

        if(cellList == null || cellList.size() == 0) {
            return null;
        }

        for(Cell cell : cellList) {
            String familyColumn = Bytes.toString(CellUtil.cloneFamily(cell));
            if(!cellMap.containsKey(familyColumn)) {
                cellMap.put(familyColumn, new HashMap<String, String>());
            }
            cellMap.get(familyColumn).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
        }

        return cellMap;
    }

    /**
     *
     * @param tableName 表名
     * @param startRow 起始行（包括）
     * @param stopRow 结束行（不包括）
     * @return Get返回Result或者List<Cell>，Scan返回ResultScanner或者List<Result>
     * @description 根据行键进行范围查询（startRow <= rowKey < stopRow）  HBase表中的行根据rowkey的ASCII码进行排序，小的在前大的在后。
     */
    public List<Result> scanRowsWithRange(String tableName, String startRow, String stopRow) {
        List<Result> rowList = null;

        TableName hTableName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(hTableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner resultScanner = table.getScanner(scan);

            rowList = new ArrayList<Result>();
            for(Result row : resultScanner) {
                rowList.add(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rowList;
    }

    /**
     *
     * @param tableName 表名
     * @param familyColumn 列族
     * @param qualifier 字段
     * @return
     * @description 使用各类过滤器扫描并获取HBase的各行（不需要固定rowKey）
     */
    public List<Result> scanRowsWithFilter(String tableName, String familyColumn, String qualifier) {
        List<Result> rowList = null;

        TableName hTableName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(hTableName);
            Scan scan = new Scan();

            // FilterList可以添加多个Filter进行过滤
            // FilterList filterList = new FilterList();

            // 前缀过滤器----针对行键（过滤结果为满足条件的所有行的所有数据）
            PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("rk"));

            // 行过滤器----针对行键（过滤结果为满足条件的所有行的所有数据）
            ByteArrayComparable binaryComparator = new BinaryComparator(Bytes.toBytes("rk_001"));
            RowFilter rowFilter = new RowFilter(CompareOperator.LESS_OR_EQUAL, binaryComparator);
            ByteArrayComparable substringComparator = new SubstringComparator("_00");
            RowFilter rowFilter2 = new RowFilter(CompareOperator.EQUAL, substringComparator);

            // 单值过滤器----针对字段值，完整匹配字节数组（过滤结果为满足条件的所有行的所有数据）
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                    Bytes.toBytes(qualifier), CompareOperator.EQUAL, Bytes.toBytes("Orange"));
            // 单值过滤器----针对字段值，匹配正则表达式
            ByteArrayComparable regexStringComparator = new RegexStringComparator("Orang.");
            SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                    Bytes.toBytes(qualifier), CompareOperator.EQUAL, regexStringComparator);
            // 单值过滤器----针对字段值，匹配是否包含子串（大小写不敏感）
            ByteArrayComparable substringComparator2 = new SubstringComparator("AN");
            SingleColumnValueFilter singleColumnValueFilter3 = new SingleColumnValueFilter(Bytes.toBytes(familyColumn),
                    Bytes.toBytes(qualifier), CompareOperator.EQUAL, substringComparator2);

            // 键值对元数据过滤----familyColumn过滤，完整匹配字节数组（过滤结果为满足条件的所有行的对应列族数据）
            ByteArrayComparable binaryComparator2 = new BinaryComparator(Bytes.toBytes("extra_info"));
            FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL, binaryComparator2);
            // 键值对元数据过滤----familyColumn过滤，前缀匹配字节数组
            ByteArrayComparable binaryPrefixComparator = new BinaryPrefixComparator(Bytes.toBytes("basic"));
            FamilyFilter familyFilter2 = new FamilyFilter(CompareOperator.EQUAL, binaryPrefixComparator);
            // 键值对元数据过滤-----qualifier过滤，完整匹配字节数组（过滤结果为满足条件的所有行的对应字段数据）
            ByteArrayComparable binaryComparator3 = new BinaryComparator(Bytes.toBytes("name"));
            QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, binaryComparator3);
            // 键值对元数据过滤-----qualifier过滤，前缀匹配字节数组
            ByteArrayComparable binaryPrefixComparator2 = new BinaryPrefixComparator(Bytes.toBytes("ev"));
            QualifierFilter qualifierFilter2 = new QualifierFilter(CompareOperator.EQUAL, binaryPrefixComparator2);

            // 基于列名(即Qualifier)前缀过滤数据的ColumnPrefixFilter（过滤结果为满足条件的所有行的对应字段数据）
            ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("na"));
            // 基于多个列名(即Qualifier)前缀过滤数据的MultipleColumnPrefixFilter（多个列名前缀过滤条件的关系为“或”，过滤结果同上）
            byte[][] prefixes = new byte[][] {Bytes.toBytes("na"), Bytes.toBytes("pr")};
            MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);

            scan.setFilter(prefixFilter);
            ResultScanner resultScanner = table.getScanner(scan);

            rowList = new ArrayList<Result>();
            for(Result result : resultScanner) {
                rowList.add(result);
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rowList;
    }

    /**
     *
     * @param args
     * @description 测试
     */
    public static void main(String[] args) {
        // zookeeper主机名
        String zksStr = "wonder1,wonder2,wonder3";
        HBaseUtil hBaseUtil = new HBaseUtil(zksStr);

        // 创建HBase表
//        String[] familyColumns = {"basic_info", "extra_info"};
//        hBaseUtil.createTable("test_order", familyColumns);

        // 删除HBase表
//        hBaseUtil.dropTable("test_order");

        // 插入一个Cell
//        hBaseUtil.insertCell("test_order", "rk_001", "extra_info", "name", "Banana");
        // 插入多个Cell（固定行固定列）
//        String[] qualifiers = {"name", "weight", "price"};
//        String[] values = {"Orange", "1.5", "6"};
//        hBaseUtil.insertCells("test_order", "rk_002", "basic_info", qualifiers, values);

        // 删除一个或者多个Cell
//        hBaseUtil.deleteCell("test_order", "rk_002", "basic_info", null);

        // 获取Cell值
//        System.out.println(hBaseUtil.getCellValue("test_order", "rk_002", "basic_info", "weight"));
        // 获取列族中Cell值
//        System.out.println(hBaseUtil.getFamilyColumnValue("test_order", "rk_002", "basic_info"));
        // 获取一行中Cell值
//        System.out.println(hBaseUtil.getRowValue("test_order", "rk_002"));

        // 根据行键进行范围查询
//        System.out.println(hBaseUtil.scanRowsWithRange("test_order", "rk_001", "rk_003"));

        // 过滤查询
//        System.out.println(hBaseUtil.scanRowsWithFilter("test_order", "basic_info", "name"));
    }
}
