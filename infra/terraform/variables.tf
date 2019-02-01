variable "project"     { default = "realtimeanalytics" }
variable "environment" { default = "prod" }
variable "aws_region"  { default = "us-east-1" }

variable "msk_instance_type"       { default = "kafka.m5.4xlarge" }
variable "msk_ebs_volume_size"     { default = 10000 }

variable "opensearch_instance_type"   { default = "r6g.2xlarge.search" }
variable "opensearch_master_user"     { sensitive = true }
variable "opensearch_master_password" { sensitive = true }

variable "scylladb_instance_type" { default = "i4i.4xlarge" }

variable "source_db_host" { description = "Hostname of the source PostgreSQL/Oracle DB" }
variable "source_db_name" { default = "orders_db" }
variable "schema_registry_url" { description = "URL of the Confluent Schema Registry" }
