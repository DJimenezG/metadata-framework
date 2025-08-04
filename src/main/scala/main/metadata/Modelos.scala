package main.metadata

case class SparkOptions(header: String, delimiter: String)

case class InputConfig(path: String, format: String)
case class Input(name: String, `type`: String, config: InputConfig, spark_options: SparkOptions)

case class AddField(name: String, expression: String)
case class TransformationConfig(fields: Option[List[AddField]], filter: Option[String])

case class Transformation(name: String, `type`: String, input: String, config: TransformationConfig)

case class OutputConfig(
  path: Option[String],
  format: Option[String],
  save_mode: String,
  partition: Option[String],
  table: Option[String],
  primary_key: Option[List[String]]
)
case class Output(name: String, `type`: String, input: String, config: OutputConfig)

case class Dataflow(
  name: String,
  inputs: List[Input],
  transformations: List[Transformation],
  outputs: List[Output]
)

case class Metadata(dataflows: List[Dataflow])
