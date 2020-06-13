package org.apache.spark.ml

import org.apache.spark.ml.feature.SQLTransformer
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.ml.PipelineUtil._

class UtilTest extends AnyFunSuite {

  val s1: SQLTransformer = new SQLTransformer("s1").setStatement("select a, b from __THIS__")
  val s2: SQLTransformer = new SQLTransformer("s2").setStatement("select *, a+b as c from __THIS__")
  val s3: SQLTransformer = new SQLTransformer("s3").setStatement("select *, c/5 as d from __THIS__")

  test("pipeline methods - simple render") {

    val pipeline = new Pipeline("pipe").setStages(Array(s1, s2, s3))

    val actual = pipeline.render()
    val expected = """|| Pipeline: pipe
                      || 0: SQLTransformer uid: s1
                      || 1: SQLTransformer uid: s2
                      || 2: SQLTransformer uid: s3""".stripMargin
    println(actual)

    assert(actual == expected)

  }

  test("pipeline methods - nested render") {

    val pipeline1 = new Pipeline("pipe").setStages(Array(s1, s2, s3))
    val pipeline2 = new Pipeline("pipe2").setStages(Array(pipeline1, s2))
    val pipeline3 = new Pipeline("pipe3").setStages(Array(pipeline2, s1, s3))

    val actual = pipeline3.render()
    val expected = """|| Pipeline: pipe3
                      || 0: Pipeline: pipe2
                      |	| 0: Pipeline: pipe
                      |		| 0: SQLTransformer uid: s1
                      |		| 1: SQLTransformer uid: s2
                      |		| 2: SQLTransformer uid: s3
                      |	| 1: SQLTransformer uid: s2
                      || 1: SQLTransformer uid: s1
                      || 2: SQLTransformer uid: s3""".stripMargin
    println(actual)

    assert(actual == expected)

  }

  test("pipeline methods - nested render with different details method") {

    val pipeline1 = new Pipeline("pipe").setStages(Array(s1, s2, s3))
    val pipeline2 = new Pipeline("pipe2").setStages(Array(pipeline1, s2))
    val pipeline3 = new Pipeline("pipe3").setStages(Array(pipeline2, s1, s3))

    val details = (tr: PipelineStage) => s"${tr.uid} -> ${tr.extractParamMap().toSeq.map(pp => s"${pp.param.name}:${pp.value}").mkString("{", ",", "}")}"
    val actual = pipeline3.render(stageDetails = details)
    val expected = """|| Pipeline: pipe3
                      || 0: Pipeline: pipe2
                      |	| 0: Pipeline: pipe
                      |		| 0: s1 -> {statement:select a, b from __THIS__}
                      |		| 1: s2 -> {statement:select *, a+b as c from __THIS__}
                      |		| 2: s3 -> {statement:select *, c/5 as d from __THIS__}
                      |	| 1: s2 -> {statement:select *, a+b as c from __THIS__}
                      || 1: s1 -> {statement:select a, b from __THIS__}
                      || 2: s3 -> {statement:select *, c/5 as d from __THIS__}""".stripMargin
    println(actual)

    assert(actual == expected)

  }

}