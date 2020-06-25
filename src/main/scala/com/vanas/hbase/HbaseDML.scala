package com.vanas.hbase

import java.util

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, CompareOperator, HBaseConfiguration, TableName}

/**
 * @author Vanas
 * @create 2020-06-24 11:04 上午 
 */
object HbaseDML {
    //1.先获取hbase的连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop130,hadoop133,hadoop134")
    val conn: Connection = ConnectionFactory.createConnection(conf)

    def main(args: Array[String]): Unit = {

        //putData("user", "1001", "info", "name", "ww")
        //deleteData("user", "1001", "info", "age")
        //getData("user", "1001", "info", "name")
        scanData("user")
        closeConnection()
    }


    def scanData(tableName: String) = {
        val table: Table = conn.getTable(TableName.valueOf(tableName))
        val scan = new Scan()
        val filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOperator.EQUAL, Bytes.toBytes("ww"))
        filter.setFilterIfMissing(true)
        scan.setFilter(filter)
        val results: ResultScanner = table.getScanner(scan)

        import scala.collection.JavaConversions._
        //从scanner拿到所有数据
        for (result <- results) {
            val cells: util.List[Cell] = result.listCells() //rowCells
            if (cells != null) {
                for (cell <- cells) {
                    println(
                        s"""
                           |row =${Bytes.toString(CellUtil.cloneRow(cell))}
                           |cf =${Bytes.toString(CellUtil.cloneFamily(cell))}
                           |name =${Bytes.toString(CellUtil.cloneQualifier(cell))}
                           |value =${Bytes.toString(CellUtil.cloneValue(cell))}
                           |""".stripMargin)
                }
            }
        }
        table.close()
    }

    def getData(tableName: String, rowKey: String, cf: String, columnName: String) = {
        val table: Table = conn.getTable(TableName.valueOf(tableName))
        val get = new Get(Bytes.toBytes(rowKey))
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName))
        val result: Result = table.get(get)


        //这个是用来在java的集合和scala的集合之间转换（隐式转换）
        import scala.collection.JavaConversions._
        val cells: util.List[Cell] = result.listCells() //rowCells
        if (cells != null) {
            for (cell <- cells) {
                //cell.getFamilyArray
                //println(Bytes.toString(CellUtil.cloneFamily(cell)))
                println(
                    s"""
                       |row =${Bytes.toString(CellUtil.cloneRow(cell))}
                       |cf =${Bytes.toString(CellUtil.cloneFamily(cell))}
                       |name =${Bytes.toString(CellUtil.cloneQualifier(cell))}
                       |value =${Bytes.toString(CellUtil.cloneValue(cell))}
                       |""".stripMargin)
            }
        }
        table.close()
    }

    def deleteData(tableName: String, rowKey: String, cf: String, columnName: String) = {
        val table: Table = conn.getTable(TableName.valueOf(tableName))
        val delete = new Delete(Bytes.toBytes(rowKey))
        //delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName))
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(columnName)) //删除所有版本
        table.delete(delete)

        table.close()
    }

    def putData(tableName: String, rowKey: String, cf: String, columnName: String, value: String) = {

        //最好先判断下
        //1.先获取到表对象，客户端到表连接
        val table: Table = conn.getTable(TableName.valueOf(tableName))

        //2.调用表对象的put
        //2.1 把需要添加的数据封装到一个Put对象 ,put ''.rowkey,''
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName), Bytes.toBytes(value))
        //put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName + "abc"), Bytes.toBytes(value + "efg"))

        //2.2 提交Put对象
        table.put(put)

        //3.关闭到table的连接
        table.close()
    }

    //4.关闭连接
    def closeConnection() = conn.close()

}
