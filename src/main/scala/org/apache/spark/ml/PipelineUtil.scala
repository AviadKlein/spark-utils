package org.apache.spark.ml

object PipelineUtil {

  implicit class ExtendedPipelineModel(val pipelineModel: PipelineModel) extends AnyVal {

    /**
     * @param indent - the amount of '\t' indents to use
     * @param preamble - the pre string to use after indents
     * @param stageDetails - an anon func to print stages, default will print the class name and uid
     * @return - a pretty string displaying the (recursive) contents of a pipeline model
     */
    def render(indent: Int=0,
               preamble: String = "| ",
               stageDetails: PipelineStage => String = s => s"${s.getClass.getSimpleName} uid: ${s.uid}"): String =
      pipelineModel.stages.zipWithIndex.foldLeft("\t"*math.max(indent-1,0) + preamble + s"Pipeline: ${pipelineModel.uid}") {
        case (acc, (stage, idx)) => acc + "\n" + (stage match {
          case s: PipelineModel => s.render(indent+1, s"| $idx: ", stageDetails)
          case s: PipelineStage if s.isInstanceOf[Transformer] => "\t"*indent + s"| $idx: " + stageDetails(s)
          case s: PipelineStage =>
            throw new Exception(s"unknown pipeline stage, got: ${s} with type: ${s.getClass.getSimpleName}")
        })
      }

  }


  implicit class ExtendedPipeline(val pipeline: Pipeline) extends AnyVal {

    /**
     * @param indent - the amount of '\t' indents to use
     * @param preamble - the pre string to use after indents
     * @param stageDetails - an anon func to print stages, default will print the class name and uid
     * @return - a pretty string displaying the (recursive) contents of a pipeline
     */
    def render(indent: Int=0,
               preamble: String = "| ",
               stageDetails: PipelineStage => String = s => s"${s.getClass.getSimpleName} uid: ${s.uid}"): String = pipeline.
      getStages.
      zipWithIndex.foldLeft("\t"*math.max(indent-1,0) + preamble + s"Pipeline: ${pipeline.uid}") {
      case (acc, (stage, idx)) => acc + "\n" + (stage match {
        case s: Pipeline =>      s.render(indent+1, s"| $idx: ", stageDetails)
        case s: PipelineModel => s.render(indent+1, s"| $idx: ", stageDetails)
        case s: PipelineStage if s.isInstanceOf[Transformer] || s.isInstanceOf[Estimator[_]] => "\t"*indent + s"| $idx: " + stageDetails(s)
        case s: PipelineStage =>
          throw new Exception(s"unknown pipeline stage, got: ${s} with type: ${s.getClass.getSimpleName}")
      })
    }

  }

}
