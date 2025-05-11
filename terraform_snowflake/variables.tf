variable "snowflake_account" {}
variable "snowflake_username" {}
variable "snowflake_password" {
  description = "Password for Snowflake user"
  type        = string
  sensitive   = true
}
variable "snowflake_role" {}
variable "snowflake_region" {}
