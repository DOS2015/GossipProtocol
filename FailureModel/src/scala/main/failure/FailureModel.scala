package scala.main.failure

import akka.actor.Cancellable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import collection.mutable.HashMap
import akka.actor._
import scala.concurrent.duration._
import actors.Actor._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import java.util.concurrent.Executors
import akka.dispatch.Futures
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import java.util.concurrent._

object FailureModel extends App {
  val system = ActorSystem("FailureModelNetwork")
  var Rumourid: Int = 0
  var deletedList = new ListBuffer[ActorRef]()
  implicit val ec = system.dispatcher
  print("Number of nodes? ")

  val noOfNodes = readInt
  val Length = noOfNodes

  print("Topology (Line / 3D / 3D-I / Full)? ")
  var topology = readLine()
  print("Val : " + topology + "\n")

  print("Protocol (Gossip / PushSum )? ")
  val Protocol = readLine()
  print("Val : " + Protocol + "\n")
  
  
  print("Enter the number of nodes you want to kill")
  var nodesToBeKilled=readInt() 

  print("Creating Nodes \n")

  print("Figuring out Neighbours \n")

  topology match {

    case "Line" =>
      var Nodes = (for { i <- 0 until Length } yield {
        val NodeRef = system.actorOf(Props[Node], s"Node_$i")
        (i, NodeRef)
      }).toMap

      var neighbours = (x: Int) => NeighboursofNode(
        for (i <- x - 1 to x + 1; if ((i) != (x))) yield {
          Nodes((i + Length) % Length)
        })
      for { i <- 0 until Length } {
        print("Sending all cells Neighbours \n")
        Nodes(i) ! neighbours(i) // send Node its' NeighboursofNode
        Nodes(i) ! sVal(i, Length)
      }

      Nodes(0) ! StartRumour(Rumourid, Protocol)
      causeFailure(Nodes, Length)

    case "3D" =>

      val Length = (math.cbrt(noOfNodes).toInt)
      print("Nodes along each edge = " + (Length + 1) + "\n")
      var Nodes = (for { i <- 0 to Length; j <- 0 to Length; k <- 0 to Length } yield {
        val NodeRef = system.actorOf(Props[Node], s"Node_$i-$j-$k")
        ((i, j, k), NodeRef)
      }).toMap

      print("Here are the nodes \n")

      for (x <- 0 to Length; y <- 0 to Length; z <- 0 to Length) {
        print("Node" + x + "." + y + "." + z + " :" + Nodes(x, y, z) + "\n")
      }

      var neighbours = (NList: ListBuffer[ActorRef]) => NeighboursofNode(NList)

      var s: Int = 0

      for (x <- 0 to Length; y <- 0 to Length; z <- 0 to Length) {
        var NeighborList = new ListBuffer[ActorRef]()

        for (i <- x - 1 to x + 1; if ((i, y, z) != (x, y, z) && (i >= 0 && i <= Length))) {
          NeighborList += Nodes(i, y, z)
        }
        for (i <- y - 1 to y + 1; if ((x, i, z) != (x, y, z) && (i >= 0 && i <= Length))) {
          NeighborList += Nodes(x, i, z)
        }
        for (i <- z - 1 to z + 1; if ((x, y, i) != (x, y, z) && (i >= 0 && i <= Length))) {
          NeighborList += Nodes(x, y, i)
        }

        Nodes(x, y, z) ! neighbours(NeighborList) // send Node its' NeighboursofNode
        Nodes(x, y, z) ! sVal(s, noOfNodes)
        s = s + 1
      }

      Nodes(0, 0, 0) ! StartRumour(Rumourid, Protocol)
      causeFailure3D(Nodes,Length)
    case "Full" =>
      var Nodes = (for { i <- 0 until Length } yield {
        val NodeRef = system.actorOf(Props[Node], s"Node_$i")
        (i, NodeRef)
      }).toMap

      var neighbours = (x: Int) => NeighboursofNode(
        for (i <- 0 to Length - 1; if ((i) != (x))) yield {
          Nodes(i)
        })
      for { i <- 0 until Length } {
        print("Sending all cells Neighbours \n")
        Nodes(i) ! neighbours(i)
        Nodes(i) ! sVal(i, Length)
      }

      Nodes(0) ! StartRumour(Rumourid, Protocol)
      causeFailure(Nodes,Length)
    case "3D-I" =>
      val Length = (math.cbrt(noOfNodes).toInt)
      print("Nodes along each edge = " + (Length + 1) + "\n")
      var Nodes = (for { i <- 0 to Length; j <- 0 to Length; k <- 0 to Length } yield {
        val NodeRef = system.actorOf(Props[Node], s"Node_$i-$j-$k")
        ((i, j, k), NodeRef)
      }).toMap

      print("Here are the nodes \n")

      for (x <- 0 to Length; y <- 0 to Length; z <- 0 to Length) {
        print("Node" + x + "." + y + "." + z + " :" + Nodes(x, y, z) + "\n")
      }

      var neighbours = (NList: ListBuffer[ActorRef]) => NeighboursofNode(NList)

      var s: Int = 0

      for (x <- 0 to Length; y <- 0 to Length; z <- 0 to Length) {

        var NeighborList = new ListBuffer[ActorRef]()

        for (i <- x - 1 to x + 1; if ((i, y, z) != (x, y, z) && (i >= 0 && i <= Length))) {
          NeighborList += Nodes(i, y, z)
        }
        for (i <- y - 1 to y + 1; if ((x, i, z) != (x, y, z) && (i >= 0 && i <= Length))) {
          NeighborList += Nodes(x, i, z)
        }
        for (i <- z - 1 to z + 1; if ((x, y, i) != (x, y, z) && (i >= 0 && i <= Length))) {
          NeighborList += Nodes(x, y, i)
        }
        //generate random neighbour 
        val rnd = new scala.util.Random
        val range = 0 to Length
        var flag = true
        while (flag) {
          val a = range(rnd.nextInt(range length))
          val b = range(rnd.nextInt(range length))
          val c = range(rnd.nextInt(range length))
          val randomNeighbor = Nodes(a, b, c)
          if (!(NeighborList.contains(randomNeighbor) || randomNeighbor.equals(Nodes(x, y, z)))) {
            NeighborList += randomNeighbor
            flag = false
          }
        }

        Nodes(x, y, z) ! neighbours(NeighborList) // send Node its' NeighboursofNode
        Nodes(x, y, z) ! sVal(s, noOfNodes)
        s = s + 1
      }
      // Selecting Node 0 as source : We can select a random node too
      Nodes(0, 0, 0) ! StartRumour(Rumourid, Protocol)
      causeFailure3D(Nodes,Length)
    case _ =>
      print("Invalid topology.Exiting...")
      system.shutdown()

  }

  def causeFailure(Nodes: Map[(Int), ActorRef], length: Int): Unit = {
    while(nodesToBeKilled>0)
    {
      var nodeToBeRemoved = util.Random.nextInt(length)
      nodesToBeKilled=nodesToBeKilled-1
      if(!(deletedList.contains(Nodes(nodeToBeRemoved)))){
      deletedList += Nodes(nodeToBeRemoved)
      Nodes(nodeToBeRemoved) ! removeTheNode();
      }
    }

  }
  
    def causeFailure3D(Nodes: Map[(Int,Int,Int), ActorRef], length: Int): Unit = {
    while(nodesToBeKilled>0)
    {
      var nodeToBeRemoved1 = util.Random.nextInt(length)
      var nodeToBeRemoved2=util.Random.nextInt(length)
      var nodeToBeRemoved3=util.Random.nextInt(length)
      if(!(deletedList.contains(Nodes(nodeToBeRemoved1,nodeToBeRemoved2,nodeToBeRemoved3)))){
      
     deletedList += Nodes(nodeToBeRemoved1,nodeToBeRemoved2,nodeToBeRemoved3)
    Nodes(nodeToBeRemoved1,nodeToBeRemoved2,nodeToBeRemoved3) ! removeTheNode();
      }
    }

  }


}

class Node extends Actor {
  var countOfNode :Int =0;
  var deletedNodes = new ListBuffer[ActorRef]()
  var neighbours: Seq[ActorRef] = Seq()
  var haveHeard: Int = 0
  var x: Int = 0
  var wforNode: Double = 1
  var sforNode: Double = 0
  var swRatio: Double = (sforNode / wforNode)
  var NoChange: Int = 0
  var Count = 1
  var y: Int = 0
  var NodesWhoRecived = collection.mutable.Set[String]()
  var sizeOfNetwork: Long = 0
  context.system.eventStream.subscribe(self, classOf[DeadLetter])
  def receive: Receive = {
    case StartRumour(id, protocol) =>
      val startTime = System.currentTimeMillis;
      print("At the Node...Got the Green Signal...am Starting ! \n")

      val NeighbourSelected = neighbours(util.Random.nextInt(neighbours.length))
      protocol match {
        case "Gossip" =>

          NeighbourSelected ! SpreadRumour(id, x, startTime, NodesWhoRecived)

        case "PushSum" =>

          NeighbourSelected ! PushRumour(sforNode, wforNode, startTime, NodesWhoRecived)

        case _ =>
          print("Invalid Protocol.Exiting...")
          context.system.shutdown()

      }

    case SpreadRumour(id, x, startTime, nodesAccessed) =>
      nodesAccessed += self.path.name
        
      if (haveHeard < 10) {
        print("%s got Something ! Spreading for ".format(self.path.name) + haveHeard + " time... \n")
        haveHeard = haveHeard + 1
        context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, SpreadRumour(id, x, startTime, nodesAccessed))
        val NeighbourSelected = neighbours(util.Random.nextInt(neighbours.length))
        NeighbourSelected ! SpreadRumour(id, x, startTime, nodesAccessed)
      } else {
        if (nodesAccessed.size > 0.8 * (sizeOfNetwork)) {
          print("The size before stopping " + nodesAccessed.size + "\n" + sizeOfNetwork)
          print("%s Old News ! am Stopping ".format(self.path.name))
          val EndTime = System.currentTimeMillis;
          print("\n Convergence time : " + (EndTime - startTime) + " ms")
          //context.system.actorSelection("/user/*") ! PoisonPill
          //context.system.actorSelection("/user/*") !stop
          context.stop(self)
          context.system.shutdown()
          
        } else {
          val NeighbourSelected = neighbours(util.Random.nextInt(neighbours.length))
          haveHeard = haveHeard + 1
          NeighbourSelected ! SpreadRumour(id, x, startTime, nodesAccessed)
        }

      }

    case PushRumour(s, w, startTime, nodesAccessed) =>

      nodesAccessed += self.path.name

      wforNode = wforNode + w
      sforNode = sforNode + s

      val swRatioNew: Double = (sforNode / wforNode)

      print("%s Diffrence this time ".format(self.path.name) + math.abs(swRatio - swRatioNew) + " \n")

      if (math.abs(swRatio - swRatioNew) < 1e-10) {
        print("%s Count incresing by 1 . Count now is ".format(self.path.name) + Count + " \n")

        Count += 1

      } else {

        Count = 0
      }

      print("%s ".format(self.path.name) + " S/W New:Old " + swRatioNew + ":" + swRatio + "\n")

      swRatio = swRatioNew

      if (Count < 3) {
        print("%s got Something ! Spreading for ".format(self.path.name) + haveHeard + " time... \n")
        val NeighbourSelected = neighbours(util.Random.nextInt(neighbours.length))
        NeighbourSelected ! PushRumour((sforNode / 2), (wforNode / 2), startTime, nodesAccessed)
        haveHeard = haveHeard + 1
      } else {

        print("%s Old News ! am Stopping ".format(self.path.name) + "at time" + System.currentTimeMillis + "\n")
        val EndTime = System.currentTimeMillis;

        print("\n Convergence time : " + (EndTime - startTime) + " ms \n \n")
        print(nodesAccessed.size)
        context.stop(self)
        context.system.shutdown()

      }

    case sVal(i, size) =>
      sforNode = i
      sizeOfNetwork = size

    case NeighboursofNode(xs) =>
      neighbours = xs
      print(" %s Recived Neighbors ".format(self.path.name) + neighbours + "\n")

    case removeTheNode() =>
      context.stop(self)
    case DeadLetter(msg, from, to) =>
      println(msg + " " + from + " " + to + "DEAD LETTERS")
      //val cancellable =context.system.scheduler.schedule(0 milliseconds,50 milliseconds,to,msg)
      countOfNode =countOfNode +1 
      //if(countOfNode>=2){
       // cancellable.cancel()
        neighbours = neighbours.filter(_ != to)
        val NeighbourSelected = neighbours(util.Random.nextInt(neighbours.length))
        NeighbourSelected ! msg      
      //}
      //else{
        //println("Trying again")
      
        //}
    case removeDeltedNode() =>
      println("Size before deleting " + neighbours.size)
    case stop =>
      println(" %s Iv been told to stop".format(self.path.name))
      context.stop(self)

  }
}

case class SpreadRumour(id: Int, x: Int, StartTime: Long, NodesWhoRecived: collection.mutable.Set[String])
case class PushRumour(s: Double, w: Double, StartTime: Long, NodesWhoRecived: collection.mutable.Set[String])
case class NeighboursofNode(xs: Seq[ActorRef])
case class StartRumour(id: Int, topology: String)
case class sVal(s: Int, size: Long)
case class removeTheNode()
case class removeDeltedNode()
case class stop()