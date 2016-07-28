
package com.ctm

import com.ctm._
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object Employee {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.master", "local[*]")
    conf.set("spark.ui.port", "36000") // Override the default port

    val sc = new SparkContext(conf.setAppName("emp"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val employee = sqlContext.read.json("/Users/jnayak/Desktop/employee")
    //employee.write.parquet("employee.parquet")
    employee.write.format("parquet").mode("overwrite")save("employee.parquet")
    val parqfile = sqlContext.read.parquet("employee.parquet")
    parqfile.registerTempTable("employee")
    val allrecords = sqlContext.sql("Select * FROM employee")
    allrecords.show()
    val showAllAgeLessThan30 = sqlContext.sql("Select * FROM employee where age <30")
    showAllAgeLessThan30.show()
  }
}
