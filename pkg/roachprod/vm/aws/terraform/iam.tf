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
		  	Sid = "BackupTestingAssume"
		  	Effect = "Allow"
		  	Principal = {
		  		AWS = "arn:aws:iam::541263489771:role/backup-testing"
		  	}
		  	Action = "sts:AssumeRole"
  		},
    	{
		  	Sid = "AcctAssume"
		  	Effect = "Allow"
		  	Principal = {
		  		AWS = "arn:aws:iam::541263489771:root"
		  	}
		  	Action = "sts:AssumeRole"
  		}
    ]
  })

  description = "Role for accessing MSK in roachtests"

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonMSKFullAccess"
  ]

  inline_policy {
    name = "more-kafka-permissions"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Sid = "MoreKafka"
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
    roachtest = "true"
  }
}
