# spark-utils
some stuff for spark ml I find useful

## Pipeline render

An implicit recursive print method to print either a `Pipeline` or `PipelineModel`

```scala
import org.apache.spark.ml.feature.SQLTransformer

import org.apache.spark.ml.PipelineUtil._

val s1: SQLTransformer = new SQLTransformer("s1").setStatement("select a, b from __THIS__")
val s2: SQLTransformer = new SQLTransformer("s2").setStatement("select *, a+b as c from __THIS__")
val s3: SQLTransformer = new SQLTransformer("s3").setStatement("select *, c/5 as d from __THIS__")

val pipeline = new Pipeline("pipe").setStages(Array(s1, s2, s3))
```
running `pipelin.render()` would return

```
| Pipeline: pipe
| 0: SQLTransformer uid: s1
| 1: SQLTransformer uid: s2
| 2: SQLTransformer uid: s3
```

Nested pipelines would look like this:
```
| Pipeline: pipe3
| 0: Pipeline: pipe2
	| 0: Pipeline: pipe
		| 0: SQLTransformer uid: s1
		| 1: SQLTransformer uid: s2
		| 2: SQLTransformer uid: s3
	| 1: SQLTransformer uid: s2
| 1: SQLTransformer uid: s1
| 2: SQLTransformer uid: s3
```
