package org.apache.flink.quickstart

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * Created by hajira on 12/28/15.
 */
object Pso3 {

  private var dim: Int = 2
  private var population: Int = 2
  def main(args: Array[String]) {


    val env = ExecutionEnvironment.getExecutionEnvironment
    val points: DataSet[PSOParticle] = getPointDataSet(env)
    val finalPops = points.iterate(2) { currentPop =>
              val newPop = points.map(new calcfit)
              newPop
    }
    finalPops.print()
    env.execute("PSO Example")
  }
  private def getPointDataSet(env: ExecutionEnvironment): DataSet[PSOParticle] = {

    val POINTS = Array.ofDim[PSOParticle](population)

    val points  = POINTS.map {
      x => new PSOParticle(dim)
      }
    env.fromCollection(points)
  }
  class PSOParticle(var dim: Int) extends Serializable {
    def this() {
      this(2)
    }

    //FlatMapFunction[(PSOParticle), (PSOParticle) ]{
    var dimension: Int = dim
    var pfit: Double = Double.MaxValue
    var fit: Double = Double.MaxValue
    var position: Array[Double] = (1 to dimension toArray) map (x => Math.random())
    var velocity: Array[Double] = (1 to dimension toArray) map (x => Math.random())
    var pbest: Array[Double] = (1 to dimension toArray) map (x => Math.random())


    def update(gbest: PSOParticle): Unit = {
     // for loops to update
    }
    def sphere(): Double = {
      // for loops to update
      var sum = 0.0
      0 until this.dimension foreach { j =>
        sum = sum + this.position(j) * this.position(j)
      }
      this.fit = sum
      this.fit
    }
    
  }
  
  final class calcfit extends RichMapFunction[PSOParticle, (PSOParticle)] {
    /** Reads the gbest values from a broadcast variable into a collection. */
    def map(p: PSOParticle): ( PSOParticle) = {
      var t = p.sphere()
      p.fit = 0 // checking here if it changes or not, it does not change in any case ..
      p
      }

    }



}
