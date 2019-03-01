# Amazon MSK (Managed Apache Kafka)

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project}-${var.environment}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_ebs_volume_size
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }
}

resource "aws_msk_configuration" "main" {
  name           = "${var.project}-config"
  kafka_versions = ["3.6.0"]

  server_properties = <<-EOF
    auto.create.topics.enable=false
    log.retention.hours=168
    log.segment.bytes=1073741824
    num.partitions=64
    default.replication.factor=3
    min.insync.replicas=2
    unclean.leader.election.enable=false
    compression.type=lz4
    message.max.bytes=10485760
    replica.fetch.max.bytes=10485760
    socket.send.buffer.bytes=102400
    socket.receive.buffer.bytes=102400
    socket.request.max.bytes=104857600
  EOF
}

resource "aws_kms_key" "msk" {
  description             = "KMS key for MSK encryption"
  deletion_window_in_days = 7
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project}-${var.environment}"
  retention_in_days = 14
}

# ── MSK Connect (Debezium CDC) ────────────────────────────────────────────────
resource "aws_mskconnect_connector" "debezium_orders" {
  name = "${var.project}-debezium-orders"

  kafkaconnect_version = "2.7.1"

  capacity {
    autoscaling {
      mcu_count        = 1
      min_worker_count = 1
      max_worker_count = 4
      scale_in_policy  { cpu_utilization_percentage = 20 }
      scale_out_policy { cpu_utilization_percentage = 80 }
    }
  }

  connector_configuration = {
    "connector.class"                            = "io.debezium.connector.postgresql.PostgresConnector"
    "tasks.max"                                  = "1"
    "database.hostname"                          = var.source_db_host
    "database.port"                              = "5432"
    "database.user"                              = "debezium_user"
    "database.password"                          = "{{resolve:secretsmanager:${var.project}/debezium-db-password}}"
    "database.dbname"                            = var.source_db_name
    "database.server.name"                       = "orders_ebs"
    "plugin.name"                                = "pgoutput"
    "publication.name"                           = "orders_pub"
    "table.include.list"                         = "public.orders,public.order_line_items,public.inventory,public.shipments,public.allocation_records"
    "topic.prefix"                               = "cdc"
    "key.converter"                              = "org.apache.kafka.connect.storage.StringConverter"
    "value.converter"                            = "io.confluent.connect.avro.AvroConverter"
    "value.converter.schema.registry.url"        = var.schema_registry_url
    "transforms"                                 = "unwrap"
    "transforms.unwrap.type"                     = "io.debezium.transforms.ExtractNewRecordState"
    "transforms.unwrap.add.fields"               = "op,table,lsn,source.ts_ms"
    "errors.tolerance"                           = "all"
    "errors.deadletterqueue.topic.name"          = "dlq.cdc_errors"
    "errors.deadletterqueue.topic.replication.factor" = "3"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.main.bootstrap_brokers_tls
      vpc {
        security_groups = [aws_security_group.msk.id]
        subnets         = module.vpc.private_subnets
      }
    }
  }

  kafka_cluster_client_authentication { authentication_type = "NONE" }
  kafka_cluster_encryption_in_transit { encryption_type     = "TLS" }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.debezium.arn
      revision = aws_mskconnect_custom_plugin.debezium.latest_revision
    }
  }

  service_execution_role_arn = aws_iam_role.msk_connect.arn
}

resource "aws_mskconnect_custom_plugin" "debezium" {
  name         = "${var.project}-debezium-plugin"
  content_type = "JAR"
  location {
    s3 {
      bucket_arn = aws_s3_bucket.flink_checkpoints.arn
      file_key   = "plugins/debezium-connector-postgres.jar"
    }
  }
}

resource "aws_iam_role" "msk_connect" {
  name               = "${var.project}-msk-connect-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kafkaconnect.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "msk_connect" {
  role       = aws_iam_role.msk_connect.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonMSKFullAccess"
}
