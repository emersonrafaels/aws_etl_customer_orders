resource "aws_s3_bucket" "bucket_etl" {
  bucket = "etl-customer-orders"
  acl = "private"

  tags = {
    Name = "ETL - GLUE JOBS - LAMBDA - STPEFUNCTION"
  }
}

resource "aws_s3_bucket_object" "bucket_etl_dirs" {
  for_each = {
    "data/" = "",
    "data/raw_data/" = "",
    "data/processed_data/" = "",
    "data/consomer_zone/" = "",
    "scripts/" = "",
    "scripts/etl_glue_scripts/" = "",
    "scripts/glue_crawlers/" = "",
    "logs/" = "",
    "temp/" = ""
  }

  bucket = aws_s3_bucket.bucket_etl.id
  key    = each.key
}
