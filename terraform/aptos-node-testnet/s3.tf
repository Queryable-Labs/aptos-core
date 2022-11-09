# Creates an s3 bucket to use internally for loading genesis data
resource "random_id" "genesis-bucket" {
  byte_length = 4
}

resource "aws_s3_bucket" "genesis" {
  bucket = "aptos-${local.workspace_name}-genesis-${random_id.genesis-bucket.hex}"
}

resource "aws_s3_bucket_public_access_block" "genesis" {
  bucket                  = aws_s3_bucket.genesis.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


data "aws_iam_policy_document" "genesis-assume-role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type = "Federated"
      identifiers = [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${module.validator.oidc_provider}"
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.validator.oidc_provider}:sub"
      # the name of the default genesis service account
      # make it available in each namespace as well
      # XXX: we should be able to specify it in every namespace
      # values = ["system:serviceaccount:*:*aptos-genesis*"]
      values = [
        "system:serviceaccount:default:genesis-aptos-genesis",
        "system:serviceaccount:default:${local.aptos_node_helm_prefix}-validator",
        "system:serviceaccount:default:${local.aptos_node_helm_prefix}-fullnode",
        ]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.validator.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "genesis" {
  statement {
    sid = "AllowS3"
    actions = [
      "s3:*",
    ]
    resources = [
      aws_s3_bucket.genesis.arn,
      "${aws_s3_bucket.genesis.arn}/*"
    ]
  }
}

resource "aws_iam_role" "genesis" {
  name                 = "aptos-node-testnet-${local.workspace_name}-genesis"
  path                 = var.iam_path
  permissions_boundary = var.permissions_boundary_policy
  assume_role_policy   = data.aws_iam_policy_document.genesis-assume-role.json
}

resource "aws_iam_role_policy" "genesis" {
  name   = "Helm"
  role   = aws_iam_role.genesis.name
  policy = data.aws_iam_policy_document.genesis.json
}

