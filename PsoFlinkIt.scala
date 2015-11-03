package org.apache.flink.quickstart

import org.apache.flink.api.scala._

import scala.collection.mutable

/**
 * Created by hajira on 10/15/15.
 */

// very basic version mainly due to moving code around to see the updates work....
object PsoFlinkIt {
  val dim: Int = 4
  var gbest: PSOParticle = new PSOParticle()
  var tbest: PSOParticle = new PSOParticle()


  class PSOParticle() {
    //FlatMapFunction[(PSOParticle), (PSOParticle) ]{
    var dimension: Int = dim
    var pfit: Double = 100000.0
    var fit: Double = 10000000.0
    var position: Array[Double] = (1 to dim toArray) map (x => Math.random())
    var velocity: Array[Double] = (1 to dim toArray) map (x => Math.random())
    var pbest: Array[Double] = (1 to dim toArray) map (x => Math.random())

    def Copy(particle: PSOParticle): Unit = {

      0 until particle.dimension foreach { a =>
        this.velocity(a) = particle.velocity(a)
        this.position(a) = particle.position(a)
      }
      this.fit = particle.fit

    }

    def stringString(): String = {
      var st = new StringBuilder()
      0 until this.dimension foreach { a =>
        st.append("\n Vel")
        st.append(this.velocity(a).toString)
        st.append("\n Pos")
        st.append(this.position(a).toString)
      }
      st.append("\n \n--Fitness =")
      st.append(this.fit.toString)
      println("updating gbbes" + st)
      var temp = new String()
      temp.addString(st)
      return temp
    }

    //    override def flatMap(particle: (PSOParticle), out: Collector[PSOParticle]): Unit = {
    //      var sum = 0.0
    //
    //      0 until particle.dimension foreach { j =>
    //
    //        sum = sum + particle.position(j) * particle.position(j)
    //      }
    //      particle.fit = sum
    //      if (particle.pfit > sum) {
    //        particle.pfit = sum
    //        particle.position.copyToArray(particle.pbest)
    //        ChkGbest(particle)
    //      }
    //      out.collect(particle)
    //    }

  }

  //override def Map ()
  def sphere(particle: PSOParticle): PSOParticle = {
    var sum = 0.0

    0 until particle.dimension foreach { j =>

      sum = sum + particle.position(j) * particle.position(j)
    }
    particle.fit = sum
    if (particle.pfit > sum) {
      particle.pfit = sum
      particle.position.copyToArray(particle.pbest)
      ChkGbest(particle)
    }
    particle

  }

  def ChkGbest(particle: PSOParticle): Unit = {
    if (particle.fit < gbest.fit) {
      gbest.Copy(particle)
      println("updating gbbest................\n" + tbest.fit.toString + gbest.fit.toString)
    }
  }
  // works !
  //  private def getPop(env: ExecutionEnvironment, size: Int): DataSet[PSOParticle] = {
  //    val pop = ((1 to size) map (x => new PSOParticle()))
  //    env.fromCollection(pop)
  //  }

  def updateP(particle: PSOParticle): PSOParticle = {
    val c1 = 1.49618
    val c2 = 1.49618
    val W = 0.7298
    0 until particle.dimension foreach { a =>
      particle.velocity(a) = W * particle.velocity(a) + (Math.random() * c1 * (gbest.position(a) - particle.position(a))) + (Math.random() * c2 * (particle.pbest(a) - particle.position(a)))
      particle.position(a) = particle.velocity(a) + particle.position(a)
    }
    return particle
  }

  def main(args: Array[String]): Unit = {
    // execution parameters
    val iter = 3
    val populationsize = 5
    val size = populationsize

    // environment settings

    val env = ExecutionEnvironment.getExecutionEnvironment
    // val population = env.fromCollection( Array[PSOParticle]= ((1 to size toArray) map (x => new PSOParticle(dim))))//:
    //val population : DataSet[PSOParticle] = getPop(env, size)
    //val pop = mutable.Iterable((1 to size) map(x => new PSOParticle()))
    //var it = asScalaIteratorConverter(pop)
    //val pop = ((1 to size) map (x => new PSOParticle())) // something wrong here????? works
    var p1 = new PSOParticle()
    var p2 = new PSOParticle()
    var p3 = new PSOParticle()
    val population = env.fromElements(p1, p2, p3)
    //val population = env.fromParallelCollection(pop)//works


    println(population.getType())


    // to DO

    var gbest: PSOParticle = new PSOParticle()
    println(gbest.stringString())
    var tbest: PSOParticle = new PSOParticle()


// Iterator try !!!!!! with inline and
    // Create initial DataSet
    val initial = env.fromElements(p1, p2, p3)

    val count = initial.iterate(20) { iterationInput: DataSet[PSOParticle] =>
      var initial2 = iterationInput.map { x => {
        var sum = 0.0
        0 until x.dimension foreach { j =>

          sum = sum + x.position(j) * x.position(j)
        }
        x.fit = sum
      }
        x
      } // not being updated
      initial2
    }
// Basic version with inline function

    0 until iter foreach { j =>
      population.map(x => {
        var sum = 0.0
        0 until x.dimension foreach { j =>

          sum = sum + x.position(j) * x.position(j)
        }
        x.fit = sum
      })
      population.map(x => updateP(_)) // NOT BEING UPDATED
      population.collect()
      population.print()
      // population.groupBy(pop[1].fit).sum("fit").print()
      println("........................................................." + gbest.stringString())

    }
    //population.map(x=>x.stringString())
    //population.writeAsCsv("test.txt","/n", "   ")
      env.execute()
  }

}
