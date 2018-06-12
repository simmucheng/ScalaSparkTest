import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql._

class JDBCSink(url:String,username:String,password:String) extends  ForeachWriter[Row]{
  var connection:Connection = _
  var statement:Statement = _
  var resultSet:ResultSet = _
  override def open(partitionId: Long, version: Long): Boolean = {
    //需要写一个jdbc的连接池
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    connection=DriverManager.getConnection(url,username,password)
    statement=connection.createStatement()
    true
  }

  override def process(value: Row): Unit = {
    var titleName=value.getAs[String]("titleName").replaceAll("[\\[\\]]","")
    var count=value.getAs[Long]("count")


    val querySql = "select 1 from webCount " +
      "where titleName = '"+titleName+"'"

    val updateSql = "update webCount set " +
      "count = "+count+" where titleName = '"+titleName+"'"

    val insertSql = "insert into webCount(titleName,count)" +
      "values('"+titleName+"',"+count+")"

    resultSet=statement.executeQuery(querySql)

    if(resultSet.next()){
      statement.executeUpdate(updateSql)
    }
    else{
      statement.execute(insertSql)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if(resultSet!=null){
      resultSet.close()
    }
    if(statement!=null){
      statement.close()
    }
    if(connection!=null){
      connection.close()
    }



  }
}
