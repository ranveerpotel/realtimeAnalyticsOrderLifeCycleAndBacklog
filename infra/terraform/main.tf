terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
  }
  backend "s3" {
    bucket = "your-terraform-state-bucket"
    key    = "realtimeanalytics/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "realtimeanalytics"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ── VPC ───────────────────────────────────────────────────────────────────────
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = "${var.project}-vpc"
  cidr = "10.0.0.0/16"

  azs              = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets   = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = false
  enable_dns_hostnames = true
  enable_dns_support   = true
}

# ── Security Groups ───────────────────────────────────────────────────────────
resource "aws_security_group" "msk" {
  name   = "${var.project}-msk-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
  ingress {
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
}

resource "aws_security_group" "scylladb" {
  name   = "${var.project}-scylladb-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 9042
    to_port     = 9042
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
  ingress {
    from_port   = 9160
    to_port     = 9160
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
  # Internal ScyllaDB gossip
  ingress {
    from_port   = 7000
    to_port     = 7001
    protocol    = "tcp"
    self        = true
  }
}

resource "aws_security_group" "api" {
  name   = "${var.project}-api-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
}

resource "aws_security_group" "alb" {
  name   = "${var.project}-alb-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── S3 for Flink checkpoints ──────────────────────────────────────────────────
resource "aws_s3_bucket" "flink_checkpoints" {
  bucket = "${var.project}-flink-checkpoints-${var.environment}"
}

resource "aws_s3_bucket_versioning" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"
    expiration { days = 7 }
  }
}
