package org.apache.flink.quickstart

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util
import java.util.Random
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by hajira on 10/15/15.
 */


object PSOStand {

  // PSO Particle Class ?? ( need class or what ? )
  class PSOParticle(dim: Int) {

    var dimension: Int = dim
    var pfit: Double = 100000.0
    var fit: Double = 10000000.0
    var position = (1 to dim toArray) map (x => Math.random())
    var velocity = (1 to dim toArray) map (x => Math.random())
    var pbest = (1 to dim toArray) map (x => Math.random())


  }

  // PSO  System
  class PSOSystem(dim: Int, size: Int, iter: Int) {
    //   PSO Parameters
    val c1: Double = 0.51
    val c2: Double = 0.51
    val inertia: Double = 0.6
    // List a1 = new util.ArrayList().asScala
    var gbest: PSOParticle = new PSOParticle(dim)
    var tbest: PSOParticle = new PSOParticle(dim)
    var pop: Array[PSOParticle] = ((1 to size toArray) map (x => new PSOParticle(dim)))

    def move(): Unit = {
      println("Moving particles")
      0 until iter foreach { j =>
        pop.map(x => sphere(x)) // find fitness
        tbest = pop.minBy(_.fit)
        if (tbest.fit < gbest.fit) {
          gbest = Copy(tbest)
          println("updating gbbest................" + tbest.fit.toString + gbest.fit.toString)
        }
        pop.map(x => updateP(x))
      }

    }
// fitness function
    def sphere(particle: PSOParticle): Unit = {
      var sum = 0.0
      0 until particle.dimension foreach { j =>
        sum = sum + particle.position(j) * particle.position(j)
      }
      particle.fit = sum
      if (particle.pfit > sum) {
        particle.pfit = sum
        particle.position.copyToArray(particle.pbest)
      }

    }
// copy for particle
    def Copy(particle: PSOParticle): PSOParticle = {
      var copy = new PSOParticle(dim)
      0 until particle.dimension foreach { a =>
        copy.velocity(a) = particle.velocity(a)
        copy.position(a) = particle.position(a)
      }
      copy.fit = particle.fit
      return copy

    }
// Update position and Velocity
    def updateP(particle: PSOParticle) {
      val c1 = 1.49618
      val c2 = 1.49618
      val W = 0.7298
      0 until particle.dimension foreach { a =>
        particle.velocity(a) = W * particle.velocity(a) + (Math.random() * c1 * (gbest.position(a) - particle.position(a))) + (Math.random() * c2 * (particle.pbest(a) - particle.position(a)))
        particle.position(a) = particle.velocity(a) + particle.position(a)
      }

    }

  }
// toString()
  def stringString(particle: PSOParticle): Unit = {
    var st = "Particle"
    0 until particle.dimension foreach { a =>
      //      st.concat("--V")
      //      st.concat(particle.velocity(a).toString)
      //      st.concat("--P")
      //      st.concat(particle.position(a).toString)
      print("--V")
      print(particle.velocity(a).toString)
      print("--P")
      print(particle.position(a).toString)
    }
    print("--Fitness =")
    println(particle.fit.toString)


  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val iter = 3000
    val dim = 400
    val populationsize = 2000
    val pso = new PSOSystem(dim, populationsize, iter)
    pso.move()

  }

}
