/*
 * Test file to write nested data to parquet
 */
package parquet

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

fun main() {
    // Nested data schema
    val schema = SchemaBuilder.record("Image").fields()
            .name("title").type()
                .nullable().stringType().noDefault()
            .name("group").type().array().items()
                .record("Group").fields()
                    .name("int").type()
                        .intType().noDefault()
                    .name("text").type()
                        .array()
                        .items()
                        .stringType()
                        .noDefault()
            .endRecord()
            .noDefault()
        .endRecord()

    // print parquet schema in json
    println(schema.toString(true))


    val file = Path("src/main/resources/test.parquet")
    val output = HadoopOutputFile.fromPath(file, Configuration())
    val writer = AvroParquetWriter.builder<GenericData.Record>(output)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withConf(Configuration())
        .withValidation(true)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

    val groupType = schema.getField("group").schema().elementType

    // Create dummy records
    val records = listOf(
        GenericData.Record(schema).apply {
            put("title", "foo")
            put("group", listOf(
                GenericData.Record(groupType).apply {
                    put("int", 1)
                    put("text", listOf("a", "b"))
                },
                GenericData.Record(groupType).apply {
                    put("int", 2)
                    put("text", listOf("x", "y"))
                }
            ))
        },
        GenericData.Record(schema).apply {
            put("title", "bar")
            put("group", listOf(
                GenericData.Record(groupType).apply {
                    put("int", 3)
                    put("text", listOf("c", "d"))
                },
                GenericData.Record(groupType).apply {
                    put("int", 4)
                    put("text", listOf("q", "z"))
                }
            ))
        },
    )

    records.forEach {
        writer.write(it)
    }
    writer.close()

    println("Write to ${file.name}")
}
