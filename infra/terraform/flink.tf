# Amazon Managed Service for Apache Flink

resource "aws_kinesisanalyticsv2_application" "order_lifecycle" {
  name                   = "${var.project}-order-lifecycle-${var.environment}"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.flink_checkpoints.arn
          file_key   = "jobs/order_lifecycle_job.zip"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type      = "CUSTOM"
        checkpointing_enabled   = true
        checkpoint_interval     = 30000
        min_pause_between_checkpoints = 10000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }

      parallelism_configuration {
        configuration_type     = "CUSTOM"
        auto_scaling_enabled   = true
        parallelism            = 4
        parallelism_per_kpu    = 1
      }
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          "KAFKA_BROKERS"      = aws_msk_cluster.main.bootstrap_brokers_tls
          "SCYLLA_HOSTS"       = join(",", aws_instance.scylladb[*].private_ip)
          "CHECKPOINT_DIR"     = "s3://${aws_s3_bucket.flink_checkpoints.bucket}/checkpoints/order-lifecycle"
          "PARALLELISM"        = "4"
        }
      }
    }

    vpc_configuration {
      security_group_ids = [aws_security_group.flink.id]
      subnet_ids         = module.vpc.private_subnets
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink_order_lifecycle.arn
  }
}

resource "aws_kinesisanalyticsv2_application" "backlog" {
  name                   = "${var.project}-backlog-${var.environment}"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.flink_checkpoints.arn
          file_key   = "jobs/backlog_job.zip"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type    = "CUSTOM"
        checkpointing_enabled = true
        checkpoint_interval   = 30000
      }
      parallelism_configuration {
        configuration_type  = "CUSTOM"
        auto_scaling_enabled = true
        parallelism         = 4
        parallelism_per_kpu = 1
      }
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          "KAFKA_BROKERS"  = aws_msk_cluster.main.bootstrap_brokers_tls
          "SCYLLA_HOSTS"   = join(",", aws_instance.scylladb[*].private_ip)
          "CHECKPOINT_DIR" = "s3://${aws_s3_bucket.flink_checkpoints.bucket}/checkpoints/backlog"
        }
      }
    }

    vpc_configuration {
      security_group_ids = [aws_security_group.flink.id]
      subnet_ids         = module.vpc.private_subnets
    }
  }
}

resource "aws_security_group" "flink" {
  name   = "${var.project}-flink-sg"
  vpc_id = module.vpc.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
}

resource "aws_iam_role" "flink" {
  name = "${var.project}-flink-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "flink" {
  role = aws_iam_role.flink.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.flink_checkpoints.arn, "${aws_s3_bucket.flink_checkpoints.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:DescribeLogGroups", "logs:DescribeLogStreams", "logs:PutLogEvents"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/flink/${var.project}-${var.environment}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "flink_order_lifecycle" {
  name           = "order-lifecycle"
  log_group_name = aws_cloudwatch_log_group.flink.name
}
