locals {
  msk_role_name = "roachprod-msk-full-access"
}

resource "aws_iam_role" "msk_full_access" {
  name = local.msk_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid = "assume-self"
        Principal = {
          AWS = [
            "arn:aws:iam::${local.account_number}:root",
            "arn:aws:iam::${local.account_number}:role/${local.msk_role_name}"
          ]
        },
      }
    ]
  })

  description = "Role for accessing MSK in roachtests"

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonMSKFullAccess"
  ]

  inline_policy {
    name = "assume-self"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action = "sts:AssumeRole"
          Effect = "Allow"
          Sid = "assume-self"
          Principal = {
            AWS = [
              "arn:aws:iam::${local.account_number}:root",
              "arn:aws:iam::${local.account_number}:role/${local.msk_role_name}"
            ]
          },
        }
      ]
    })
  }

  inline_policy {
    name = "more-kafka-permissions"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Sid = "allow-more-kafka"
          Action = [
            "kafkaconnect:*",
            "kafka-cluster:*"
          ]
          Resource = "*"
        }
      ]
    })
  }

  tags = {
    tag-key = "tag-value"
  }
}
