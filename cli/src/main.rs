use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use clap::{App, Arg};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder};
use arrow::record_batch::RecordBatchReader;
use pgpq::ArrowToPostgresBinaryEncoder;
use std::fs::File;
use std::io::{self, Write};
use arrow::datatypes::{Schema};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("pgpq")
        .version("0.1.0")
        .about("Converts Parquet files to PostgreSQL binary format")
        .arg(
            Arg::with_name("input")
                .short('i')
                .long("input")
                .value_name("FILE")
                .help("Input Parquet file (use '-' for stdin)")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let input = matches.value_of("input").unwrap();

    let (batches, schema) = read_parquet_from_file(input);
    let mut encoder = ArrowToPostgresBinaryEncoder::try_new(&schema).unwrap();
    let mut buf = BytesMut::new();
    encoder.write_header(&mut buf);

    io::stdout().write_all(&buf)?;
    buf.clear();

    for batch in batches {
        encoder.write_batch(&batch, &mut buf).unwrap();
        io::stdout().write_all(&buf)?;
        buf.clear();
    }
    encoder.write_footer(&mut buf).unwrap();
    io::stdout().write_all(&buf)?;
    buf.clear();

    Ok(())
}

fn read_parquet_from_file(input: &str) -> (Vec<RecordBatch>, Schema) {
    // let mut file = download_dataset();
    let file = File::open(input).expect("failed to open file");

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();
    let schema = Schema::new(reader.schema().fields().clone());
    let data: Vec<RecordBatch> = reader.map(|v| v.unwrap()).collect();
    (data, schema)
}

