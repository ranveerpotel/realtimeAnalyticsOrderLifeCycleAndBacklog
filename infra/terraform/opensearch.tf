# Amazon OpenSearch Service

resource "aws_opensearch_domain" "main" {
  domain_name    = "${var.project}-${var.environment}"
  engine_version = "OpenSearch_2.13"

  cluster_config {
    instance_type            = var.opensearch_instance_type
    instance_count           = 3
    zone_awareness_enabled   = true
    zone_awareness_config {
      availability_zone_count = 3
    }
    dedicated_master_enabled = true
    dedicated_master_type    = "r6g.large.search"
    dedicated_master_count   = 3
  }

  ebs_options {
    ebs_enabled = true
    volume_type = "gp3"
    volume_size = 200
    throughput  = 250
    iops        = 3000
  }

  encrypt_at_rest            { enabled = true }
  node_to_node_encryption    { enabled = true }
  domain_endpoint_options    { enforce_https = true }

  advanced_security_options {
    enabled                        = true
    anonymous_auth_enabled         = false
    internal_user_database_enabled = true
    master_user_options {
      master_user_name     = var.opensearch_master_user
      master_user_password = var.opensearch_master_password
    }
  }

  vpc_options {
    subnet_ids         = slice(module.vpc.private_subnets, 0, 3)
    security_group_ids = [aws_security_group.opensearch.id]
  }

  log_publishing_options {
    log_type                 = "INDEX_SLOW_LOGS"
    cloudwatch_log_group_arn = "${aws_cloudwatch_log_group.opensearch.arn}:*"
  }

  log_publishing_options {
    log_type                 = "SEARCH_SLOW_LOGS"
    cloudwatch_log_group_arn = "${aws_cloudwatch_log_group.opensearch.arn}:*"
  }

  auto_tune_options {
    desired_state       = "ENABLED"
    rollback_on_disable = "NO_ROLLBACK"
  }
}

resource "aws_security_group" "opensearch" {
  name   = "${var.project}-opensearch-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
  ingress {
    from_port   = 9200
    to_port     = 9200
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
}

resource "aws_cloudwatch_log_group" "opensearch" {
  name              = "/aws/opensearch/${var.project}-${var.environment}"
  retention_in_days = 14
}

# ── ScyllaDB on EC2 (no managed AWS service) ───────────────────────────────
data "aws_ami" "scylladb" {
  most_recent = true
  owners      = ["158855661827"]  # ScyllaDB official AMI account
  filter {
    name   = "name"
    values = ["ScyllaDB-5.4.*"]
  }
}

resource "aws_instance" "scylladb" {
  count = 3

  ami                    = data.aws_ami.scylladb.id
  instance_type          = var.scylladb_instance_type
  subnet_id              = module.vpc.private_subnets[count.index]
  vpc_security_group_ids = [aws_security_group.scylladb.id]
  iam_instance_profile   = aws_iam_instance_profile.scylladb.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 50
  }

  ebs_block_device {
    device_name = "/dev/xvdb"
    volume_type = "io2"
    volume_size = 3750
    iops        = 100000
    encrypted   = true
    kms_key_id  = aws_kms_key.msk.arn
  }

  user_data = base64encode(templatefile("${path.module}/templates/scylladb-init.sh.tpl", {
    seeds = "",  # populated after first node is up
    dc    = var.aws_region,
  }))

  tags = { Name = "${var.project}-scylladb-${count.index}" }
}

resource "aws_iam_instance_profile" "scylladb" {
  name = "${var.project}-scylladb-profile"
  role = aws_iam_role.scylladb.name
}

resource "aws_iam_role" "scylladb" {
  name = "${var.project}-scylladb-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "scylladb_ssm" {
  role       = aws_iam_role.scylladb.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
