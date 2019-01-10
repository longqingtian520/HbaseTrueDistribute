package com.criss.wang.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDemo {
	
	public static Configuration conf;
	static {
		conf = HBaseConfiguration.create();
	}
	
	// 判断表是否存在
	public static boolean isExist(String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		return admin.tableExists(TableName.valueOf(tableName));
	}
	
	// 创建表
	public static void createTable(String tableName, String... columnFamily) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		if(isExist(tableName)) {
			System.out.println("" + tableName + "已存在");
		}else {
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			// 创建多个列族
			for(String cf:columnFamily) {
				descriptor.addFamily(new HColumnDescriptor(cf));
			}
			admin.createTable(descriptor);
			System.out.println("表" + tableName + "创建成功");
		}
	}
	
	// 删除表
	public static void dropTable(String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		if(isExist(tableName)) {
			if(!admin.isTableDisabled(TableName.valueOf(tableName))){
				admin.disableTable(TableName.valueOf(tableName));
			}
			admin.deleteTable(TableName.valueOf(tableName));
			System.out.println("删除成功");
		}else {
			System.out.println("不存在");
		}
	}
	
	// 向表中插入数据
	public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
		// 创建HTable对象
		HTable table = (HTable) ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		System.out.println("添加数据成功");
	}
	
	// 刪除一行數據
	public static void deleteRowData(String tableName, String rowKey, String columnFamily) throws IOException {
		// 创建HTable对象
		HTable table = (HTable) ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		System.out.println("刪除成功");
	}
	
	// 扫描全部数据
	public static  void getAllRows(String tableName) throws IOException {
		HTable table = (HTable) ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result : resultScanner) {
			result.getRow(); // 获取rowKey
			Cell[] cells = result.rawCells();
			for(Cell cell:cells) {
				System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
				System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("值" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
	}
	
	// 获取一个具体的数据
	public static void getRowData(String tableName, String rowKey) throws IOException {
		HTable table = (HTable) ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		for(Cell cell:cells) {
			System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("值" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}
	
	
	
	public static void main(String[] args) throws Exception{
		System.out.println(isExist("student"));
//		createTable("emp", "personal", "professional");
//		dropTable("emp");
		addRowData("emp", "1", "professional", "major", "computer");
		addRowData("emp", "1", "professional", "grade", "90");
//		addRowData("emp", "2", "personal", "name", "wang");
//		addRowData("emp", "2", "personal", "city", "jinan");
//		deleteRowData("emp", "1", "personal");
//		getAllRows("emp");
		getRowData("emp", "1");
	}
	
	

}
