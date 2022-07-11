import Connections.SparkConnect
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.sql.{Connection, DriverManager}

object main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkConnect.createSparkSession()

    showMenu()

    // Prints opening messages. Asks for user to login to begin.
    def showMenu() {
      println("Welcome to NBA Database")
      println("Login with your username and password to get started.")
      println("Please enter your username: ")
      val username = scala.io.StdIn.readLine()
      println("Please enter your password: ")
      val password = scala.io.StdIn.readLine()

      if (checkLogin(username, password)) {
        println("Access granted")
        checkOptions(username, password)

      }
      else {
        println("Username or password incorrect. Please try again.")

        println("Please enter your username: ")
        val username = scala.io.StdIn.readLine()
        println("Please enter your password: ")
        val password = scala.io.StdIn.readLine()

        if (checkLogin(username, password)) {
          println("Access granted")
          checkOptions(username, password)

        }
      }
    }

    //Checks login information against the database.
    def checkLogin(username: String, password: String): Boolean = {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = "My$qL123"

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM users WHERE username = '" + username + "' AND password = '" + password + "'")
      resultSet.next()
    }

    //If login info is present, user will be given the option to update or continue.
    //If the user wishes to update, prompt user for new login info and update MySQL database.
    def checkOptions(username: String, password: String) {
      println("[1] - Update username & password information")
      println("[2] - Continue to user options")
      val infoCheck = scala.io.StdIn.readLine()
      if (infoCheck == "1") {
        println("What would you like your new username to be? ")
        val newUser = scala.io.StdIn.readLine()
        println("What would you like your new password to be? ")
        val newPass = scala.io.StdIn.readLine()
        updateLogin(username, password, newUser, newPass)
        println("Changes saved.")
        checkPermissions(newUser, newPass)
      }
      else {
        checkPermissions(username, password)
      }
    }

    //Updates MySQL database with new login information.
    def updateLogin(username: String, password: String, newUser: String, newPass: String): Unit = {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = "My$qL123"
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)

      val Statement = connection.createStatement()
      val resultSet = Statement.executeQuery("SELECT * FROM users WHERE username = '" + username + "' AND password = '" + password + "'")
      resultSet.next()
      val user_ID = resultSet.getInt("user_ID")

      val statement = connection.createStatement()
      val sql = statement.executeUpdate("UPDATE users SET username = '" + newUser + "' , password = '" + newPass + "' WHERE user_ID = " + user_ID + "")
    }

    def createUser(): Unit = {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = "My$qL123"

      println("Create a new Username")
      val userName = scala.io.StdIn.readLine()

      println("Create a new Password")
      val userPassword = scala.io.StdIn.readLine()

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)
      val statement = connection.createStatement()
      val sql = statement.executeUpdate("INSERT INTO users.users(username, password, admin) VALUES('" + userName + "', '" + userPassword + "', '" + 0 + "')")
    }

    def deleteUser(): Unit = {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = "My$qL123"

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)
      val statement = connection.createStatement()
      //val sql = statement.executeQuery("SELECT user_ID, username FROM users.users")


      println("Select the user ID of the user you would like to delete")
      val userID = scala.io.StdIn.readLine()
      val sql2 = statement.executeUpdate("DELETE FROM users.users WHERE (user_ID = '" + userID + "')")

    }

    //Checks assigned permissions based on login information given by user.
    def checkPermissions(username: String, password: String) {
      val driver = "com.mysql.cj.jdbc.Driver"
      val url = "jdbc:mysql://localhost:3306/users"
      val u = "root"
      val p = "My$qL123"

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, u, p)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM users WHERE username = '" + username + "' AND password = '" + password + "'")
      while (resultSet.next()) {
        val admin = resultSet.getInt("admin")
        if (admin == 0) {
          println("Logged in as BASIC user")
          val df1 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/wpena/csvfiles/warriorsRoster.csv")
          val df2 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/wpena/csvfiles/playoffPerGameStats.csv")
          val df3 = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/wpena/csvfiles/rSPerGameStats.csv")
          df1.createOrReplaceTempView("teamRoster")
          df2.createOrReplaceTempView("playoffStats")
          df3.createOrReplaceTempView("rSStats")
          basicOptions()
          rerunBasicMenu()
        }
        else {
          println("Logged in as ADMIN user")
          adminOptions()
          rerunAdminMenu()
        }
      }
    }

    //noinspection ScalaUnreachableCode
    def basicOptions(): Unit ={
      spark.sql("SELECT * FROM teamRoster").show()

      println("Select what you would like to view")
      println("[1] - How does Warriors Regular Season Vs Playoff Points Per Game differ & which players improved in the post-season")
      println("[2] - Which players increased their Free Throw Attempts Per Game?")
      println("[3] - How does team Regular Season Vs Playoff Field Goal Attempts Per Game differ?")
      println("[4] - Did the teams 3-Point Attempts increase or decrease as a whole from the regular season to the playoffs?")
      println("[5] - Which players increased their 2-Point Attempts Per Game?")
      println("[6] - How well did the Warriors take care of the ball in the Regular Season & in the Playoffs?")
      println("[0] - Exit")
      val userInput = scala.io.StdIn.readLine()

      do{
        if (userInput == "1") {
          spark.sql("SELECT tr.Player, rs.regular_season_PPG, ps.playoff_PPG From teamRoster as tr join rSStats as rs on (tr.Num = rs.Num) join playoffStats as ps on (tr.Num = ps.Num)").show()
          return
        }
        else if (userInput == "2") {
          spark.sql("SELECT tr.Player, rs.regular_season_FTA, ps.playoff_FTA From teamRoster as tr join rSStats as rs on (tr.Num = rs.Num) join playoffStats as ps on (tr.Num = ps.Num)").show()
          return
        }
        else if (userInput == "3") {
          spark.sql("SELECT tr.Player, rs.regular_season_FGA, ps.playoff_FGA From teamRoster as tr join rSStats as rs on (tr.Num = rs.Num) join playoffStats as ps on (tr.Num = ps.Num)").show()
          return
        }
        else if (userInput == "4") {
          spark.sql("SELECT tr.Player, rs.regular_season_3PA, ps.playoff_3PA From teamRoster as tr join rSStats as rs on (tr.Num = rs.Num) join playoffStats as ps on (tr.Num = ps.Num)").show()
          return
        }
        else if (userInput == "5") {
          spark.sql("SELECT tr.Player, rs.regular_season_2PA, ps.playoff_2PA From teamRoster as tr join rSStats as rs on (tr.Num = rs.Num) join playoffStats as ps on (tr.Num = ps.Num)").show()
          return
        }
        else if (userInput == "6") {
          spark.sql("SELECT tr.Player, rs.regular_season_TOV, ps.playoff_TOV From teamRoster as tr join rSStats as rs on (tr.Num = rs.Num) join playoffStats as ps on (tr.Num = ps.Num)").show()
          return
        }
        else {
          println("Invalid Input. Please try again")
          return
        }
      }while(userInput != "0")
      sys.exit(0)
    }

    def rerunBasicMenu(): Unit ={
      println("[1] - View Questions")
      println("[0] - Exit")
      val answer = scala.io.StdIn.readLine()

      if(answer == "1"){
        basicOptions()
        rerunBasicMenu()
      }
      else if (answer == "0") {
        println("Goodbye!")
        sys.exit(0)
      }
      else {
        println("Invalid Input. Please try again")
        rerunBasicMenu()
      }
    }

    //noinspection ScalaUnreachableCode
    def adminOptions(): Unit ={
      println("Select what you would like to do")
      println("[1] - Create User")
      println("[2] - Delete User")
      println("[0] - Exit")
      val userInput = scala.io.StdIn.readLine()

      do{
        if(userInput == "1"){
          createUser()
          return
        }
        else if(userInput == "2"){
          deleteUser()
          return
        }
        else {
          println("Invalid Input. Try again.")
          return
        }
      }while(userInput != "0")

    }

    def rerunAdminMenu(): Unit ={
      println("Select what you would like to do")
      println("[1] - Create/Delete another user ")
      println("[2] - Logout")
      println("[0] - Exit")
      val answer = scala.io.StdIn.readLine()

      if(answer == "1"){
        adminOptions()
      }
      else if(answer == "2"){
        showMenu()
      }
      else {
        println("Goodbye!")
      }
    }

  }
}
