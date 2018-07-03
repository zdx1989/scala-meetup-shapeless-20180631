package scala.meetup.jdbc.shapeless

import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}

import shapeless._

import scala.annotation.tailrec

/**
  * Created by zhoudunxiong on 2018/6/24.
  */
object Database {

  def query(sql: String, conn: Connection): List[List[String]] = {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement()
      stmt.executeQuery(sql).rows
    } catch {
      case e: SQLException =>
        List()
    } finally {
      if (stmt != null) stmt.close()
    }
  }

  implicit class ResultSetOp(rs: ResultSet) {

    private val columnLength: Int = rs.getMetaData.getColumnCount

    def rows: List[List[String]] = {
      @tailrec
      def loop(rs: ResultSet, res: List[List[String]]): List[List[String]] =
        if (!rs.next()) res
        else {
          val row = (1 to columnLength).map(rs.getString).toList
          loop(rs, res :+ row)
        }
      loop(rs, Nil)
    }
  }

  def query1[A](sql: String, conn: Connection)(implicit mapper: Mapper[A]) : List[A] = {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement()
      stmt.executeQuery(sql).rows.map(mapper.to)
    } catch {
      case e: SQLException =>
        List()
    } finally {
      if (stmt != null) stmt.close()
    }
  }

  def conn: Connection = {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection("jdbc:h2:~/test")
  }

  case class Student(id: Long, name: String, score: Int)

  object Student extends Mapper[Student] {
    override def to(li: List[String]): Student = {
      val id = li(0).toLong
      val name = li(1).toString
      val score = li(2).toInt
      Student(id, name, score)
    }
  }

  private def list2Student(li: List[String]): Student = {
    val id = li(0).toLong
    val name = li(1).toString
    val score = li(2).toInt
    Student(id, name, score)
  }

  case class Book(id: Long, name: String, price: Int)

  private def list2Book(li: List[String]): Book = {
    val id = li(0).toLong
    val name = li(1)
    val street = li(2).toInt
    Book(id, name, street)
  }

  object Book extends Mapper[Book] {
    override def to(li: List[String]): Book = {
      val id = li(0).toLong
      val name = li(1)
      val price = li(2).toInt
      Book(id, name, price)
    }
  }


  def main(args: Array[String]): Unit = {
    import IOUtils._
    val sql1 = s"SELECT * FROM student"
    val rows1 = using(conn)(query(sql1, _))
    val students: List[Student] = rows1.map(list2Student)

    val sql2 = s"SELECT * FROM book"
    val rows2 = using(conn)(query(sql2, _))
    val books: List[Book] = rows2.map(list2Book)


    val stidtents1: List[Student] = using(conn)(query(sql1, _)).map(Student.to)
    val books1: List[Book] = using(conn)(query(sql2, _)).map(Book.to)


    val students2: List[Student] = using(conn)(query1[Student](sql1, _))
    val books2: List[Book] = using(conn)(query1[Book](sql2, _))

    val studengGeneric: List[Long :: String :: Int :: HNil] =
      using(conn)(query1[Long :: String :: Int :: HNil](sql1, _))
    val students3: List[Student] = studengGeneric.map(s => Generic[Student].from(s))

    val bookGeneric: List[Long :: String :: Int :: HNil] =
      using(conn)(query1[Long :: String :: Int :: HNil](sql2, _))
    val books3: List[Book] = bookGeneric.map(b => Generic[Book].from(b))

    case class Teacher(id: Long, name: String, onHoliday: Boolean)
    val sql3 = s"SELECT * FROM teacher"
    val teacherGeneric: List[Long :: String :: Boolean :: HNil] =
      using(conn)(query1[Long :: String :: Boolean :: HNil](sql3, _))
    val teachers: List[Teacher] = teacherGeneric.map(t => Generic[Teacher].from(t))

    val students4: List[Student] = using(conn)(query1[Student](sql1, _))
    val book4: List[Book] = using(conn)(query1[Book](sql2, _))
  }
}
