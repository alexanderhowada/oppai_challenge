variable "AWS_SUBNET_ID" {}
variable "AWS_ELASTIC_ALLOC_ID" {}
variable "AWS_VPC_ID" {}
variable "AWS_NETWORK_PREFIX_ID" {}
variable "AWS_ENDPOINT_ID" {}

resource "aws_vpc" "databricks_vpc" {
    cidr_block="10.68.0.0/16"
    lifecycle {
        prevent_destroy = true
        ignore_changes = [
            "tags", "tags_all"
        ]
    }
}

resource "aws_nat_gateway" "databricks_nat" {
    allocation_id = var.AWS_ELASTIC_ALLOC_ID
    connectivity_type = "public"
    subnet_id=var.AWS_SUBNET_ID
    private_ip="10.68.0.223"
    tags={
        "Name": "databricks_nat",
        "resource": "databricks"
    }
}

resource "aws_route_table" "databricks_nat_route" {
    vpc_id=var.AWS_VPC_ID
#     route {
#         cidr_block = "0.0.0.0/0"
#         gateway_id = aws_nat_gateway.databricks_nat.id
#     }
    route {
        cidr_block = "10.68.0.0/16"
        gateway_id = "local"
    }
    lifecycle {
        prevent_destroy = true
    }
}
# resource "aws_route_table_association" "databricks_nat_route" {
#     route_table_id = aws_route_table.databricks_nat_route.id
#     gateway_id = aws_nat_gateway.databricks_nat.id
# }

resource "aws_route" "databricks_public_route" {
    route_table_id = aws_route_table.databricks_nat_route.id
    destination_cidr_block="0.0.0.0/0"
    gateway_id = aws_nat_gateway.databricks_nat.id
}

resource "aws_vpc_endpoint_route_table_association" "databricks_route_vpc_endpoint" {
    route_table_id  = aws_route_table.databricks_nat_route.id
    vpc_endpoint_id = var.AWS_ENDPOINT_ID
}
