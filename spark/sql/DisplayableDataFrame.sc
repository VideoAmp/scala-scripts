// A Jupyter Scala script which adds the display() and display(count: Int) methods to the Spark
// DataFrame type. These methods publish a nicely formatted HTML table to the notebook.
//
// Load with the `interp.load.module` method.

import org.apache.spark.sql.DataFrame

implicit class DisplayableDataFrame(df: DataFrame) {
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._

  def display(): Unit = display(20)

  def display(numRows: Int): Unit = {
    def textAlign(fieldType: DataType): String = fieldType match {
      case _: ArrayType => "left"
      case _: MapType => "left"
      case _: StructType => "left"
      case _ => "right"
    }

    val fieldTypes = df.schema.fields.map(_.dataType)
    val thead =
      df.schema.fieldNames.zip(fieldTypes).map { case (name, fieldType) =>
        s"""<th style="text-align: ${textAlign(fieldType)}">$name</th>"""
      }.mkString("<thead><tr>", "", "</tr></thead>")
    val rows = df.take(numRows + 1)
    val tbody =
      rows.take(numRows).map(_.toSeq.zip(fieldTypes).map {
        case (value, fieldType) =>
          s"""<td style="vertical-align: top; text-align: ${textAlign(fieldType)}">""" +
            formatValue(value, 1) + "</td>"
      }.mkString("<tr>", "", "</tr>")).mkString("<tbody>", "", "</tbody>")
    val table = s"<table>$thead$tbody</table>"
    publish.html(table)
    if (rows.length > numRows) {
      publish.html(s"<p>display truncated to $numRows rows</p>")
    }
  }

  private def formatValue(value: Any, depth: Int): String = value match {
    case null => "null"
    case values: Seq[_] =>
      values.map(formatValue(_, depth + 1)).mkString("[", ", ", "]")
    case map: Map[_, _] =>
      formatMapLike(map.map {
        case (k, v) => formatValue(k, depth + 1) + " -> " + formatValue(v, depth + 1)
      }, depth)
    case row: Row =>
      formatMapLike(row.schema.fieldNames.zip(row.toSeq).map {
        case (fieldName, fieldValue) =>
          fieldName + ": " + formatValue(fieldValue, depth + 1)
      }, depth)
    case _ => value.toString
  }

  private def formatMapLike(iterable: Iterable[_], depth: Int): String = {
    val isEmptyOrSingleton = if (iterable.isEmpty) {
      true
    } else {
      val iterator = iterable.iterator
      iterator.next
      !iterator.hasNext
    }
    val (prefix, suffix) = if (isEmptyOrSingleton) {
      ("[", "]")
    } else {
      ("[<br/>" + "&nbsp;&nbsp;" * depth, "<br/>" + "&nbsp;&nbsp;" * (depth - 1) + "]")
    }
    iterable.mkString(prefix, ",<br/>" + "&nbsp;&nbsp;" * depth, suffix)
  }
}
