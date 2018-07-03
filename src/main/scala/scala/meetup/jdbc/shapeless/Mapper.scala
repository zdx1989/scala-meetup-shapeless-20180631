package scala.meetup.jdbc.shapeless

import shapeless.Generic

import scala.meetup.jdbc.shapeless.Database.{Book, Student}


/**
  * Created by zhoudunxiong on 2018/6/24.
  */
trait Mapper[A] {

  def to(li: List[String]): A
}

object Mapper {

  def apply[A](implicit ma: Mapper[A]): Mapper[A] = ma

  def instance[A](f: List[String] => A): Mapper[A] = new Mapper[A] {
    override def to(li: List[String]): A = f(li)
  }


  import shapeless.{HNil, ::, HList}


//  implicit val studentMapper: Mapper[Student] = instance { li =>
//    val id = li(0).toLong
//    val name = li(1).toString
//    val score = li(2).toInt
//    Student(id, name, score)
//  }
//
//  implicit val bookMapper: Mapper[Book] = instance { li =>
//    val id = li(0).toLong
//    val name = li(1)
//    val price = li(2).toInt
//    Book(id, name, price)
//  }

//  implicit val hListMapper: Mapper[Long :: String :: Int :: HNil] = instance { li =>
//    li(0).toLong :: li(1) :: li(2).toInt :: HNil
//  }


  implicit val intMapper: Mapper[Int] = instance(_.head.toInt)

  implicit val longMapper: Mapper[Long] = instance(_.head.toLong)

  implicit val stringMapper: Mapper[String] = instance(_.head)

  implicit val booleanMapper: Mapper[Boolean] = instance(li => if (li.head == "0") false else true)

  implicit val hNilMapper: Mapper[HNil] = instance(li => HNil)

  implicit def hListMapper[T, H <: HList](implicit ma: Mapper[T],
                                          mb: Mapper[H]): Mapper[T :: H] = instance {
    case Nil => throw new IllegalArgumentException(s"The empty List cannot be converted to HList")
    case li: List[String] => ma.to(List(li.head)) :: mb.to(li.tail)
  }

  implicit def genericMapper[A, R](implicit
                                   gen: Generic[A] { type Repr = R },
                                   mr: Mapper[R]): Mapper[A] =
    instance { li => gen.from(mr.to(li)) }
}
