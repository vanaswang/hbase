package com.vanas.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes


/**
 * @author Vanas
 * @create 2020-06-24 9:22 上午 
 */
object HbaseDDL {
    //1.先获取hbase的连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop130,hadoop133,hadoop134")
    val conn: Connection = ConnectionFactory.createConnection(conf)

    def main(args: Array[String]): Unit = {

        //println(tableExists("user"))

        createTable("hbase1", "cf1", "cf2")

        //createNS("abc")
        closeConnection()

    }

    def createNS(name: String) = {
        val admin: Admin = conn.getAdmin
        if (!nsExists(name)) {
            val nd: NamespaceDescriptor.Builder = NamespaceDescriptor.create(name)
            admin.createNamespace(nd.build())
        } else {
            println(s"创建的命名空间：${name}已经存在")
        }

        admin.close()
    }

    def nsExists(name: String): Boolean = {
        val admin: Admin = conn.getAdmin
        val nss: Array[NamespaceDescriptor] = admin.listNamespaceDescriptors()
        val r: Boolean = nss.map(_.getName).contains(name)
        admin.close()
        r

    }

    def deleteTable(name: String) = {
        val admin: Admin = conn.getAdmin

        if (tableExists(name)) {
            admin.disableTable(TableName.valueOf(name))
            admin.deleteTable(TableName.valueOf(name))
        }
        admin.close()
    }

    /**
     * 创建指定编表
     *
     * @param name
     */
    def createTable(name: String, cfs: String*): Boolean = {
        val admin: Admin = conn.getAdmin
        val tableName = TableName.valueOf(name)

        if (tableExists(name)) return false

        val td = TableDescriptorBuilder.newBuilder(tableName)


        cfs.foreach(cf => {
            val cfd = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(cf))
                    .build()
            td.setColumnFamily(cfd)
        })

        //admin.createTable(td.build())
        val splites = Array(Bytes.toBytes("aaa"), Bytes.toBytes("bbb"), Bytes.toBytes("ccc"))

        admin.createTable(td.build(), splites)
        admin.close()
        true
    }

    /**
     * 判断表是否存在
     *
     * @param name
     * @return
     */
    def tableExists(name: String): Boolean = {

        //2.获取管理对象 Admin
        val admin: Admin = conn.getAdmin

        //3.利用Admin进行各种操作
        val tableName = TableName.valueOf(name)
        val b: Boolean = admin.tableExists(tableName)

        //4.关闭Admin
        admin.close()
        b
    }

    //4.关闭连接
    def closeConnection() = conn.close()

}
