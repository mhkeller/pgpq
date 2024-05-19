// https://arrow.apache.org/docs/cpp/parquet.html
fn read_parquet_to_arrow(file_path: &str) -> arrow::error::Result<Vec<RecordBatch>> {
	// #include "arrow/io/api.h"
	// #include "arrow/parquet/arrow/reader.h"

	arrow::MemoryPool* pool = arrow::default_memory_pool();

	// Configure general Parquet reader settings
	auto reader_properties = parquet::ReaderProperties(pool);
	reader_properties.set_buffer_size(4096 * 4);
	reader_properties.enable_buffered_stream();

	// Configure Arrow-specific Parquet reader settings
	auto arrow_reader_props = parquet::ArrowReaderProperties();
	arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

	parquet::arrow::FileReaderBuilder reader_builder;
	ARROW_RETURN_NOT_OK(
			reader_builder.OpenFile(file_path, /*memory_map=*/false, reader_properties));
	reader_builder.memory_pool(pool);
	reader_builder.properties(arrow_reader_props);

	std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
	ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

	std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
	ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rb_reader));

	// Return the batches
	let mut batches = Vec::new();
	while let Some(maybe_batch) = rb_reader.next() {
		match maybe_batch {
			Ok(batch) => batches.push(batch),
			Err(error) => return Err(error.into()),
		}
	}
	Ok(batches)

	// for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
	// 	// Operate on each batch...
	// }
}

fn main() -> arrow::error::Result<()> {
		let file_path = std::env::args().nth(1).expect("no file given");

		// TODO, read in schema and create table
    let record_batches = read_parquet_to_arrow(file_path)?;

    for batch in &record_batches {
			// TODO Import the encoder module and convert to binary
      // println!("Number of rows in current batch: {}", batch.num_rows());
    }

    Ok(())
}
