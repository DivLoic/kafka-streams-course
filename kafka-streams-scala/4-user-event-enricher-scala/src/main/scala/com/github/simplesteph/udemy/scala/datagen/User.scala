package com.github.simplesteph.udemy.scala.datagen


case class User(login: String, firstName: String, lastName: Option[String] = None, email: Option[String] = None)

object User {

  def apply(login: String, firstName: String, lastName: String, email: String): User =
    new User(login, firstName, Some(lastName), Some(email))

  def apply(login: String, firstName: String, lastName: String): User =
    new User(login, firstName, Some(lastName))
}
