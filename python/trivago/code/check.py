import click
import great_expectations as ge
import pyarrow.parquet as pq
import s3fs

@click.command()
@click.option("--date", "-d", help='Date formated as %Y-%m-%d')
@click.option("--bucket", "-b", help='S3 bucket where output is stored')
def sanity_check(date, bucket):
    s3 = s3fs.S3FileSystem()

    output = ge.from_pandas(
        pq.ParquetDataset(f's3://{bucket}/usp/output/date={date}', filesystem=s3).read_pandas().to_pandas()
    )

    output.expect_column_values_to_be_in_set('accommodation_ns', [100])

    status = output.validate()
    print(status)

    if not status['success']:
        raise ValueError


if __name__ == '__main__':
    sanity_check()
