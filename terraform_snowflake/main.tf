terraform {
  required_providers {
    snowflake = {
      source = "snowflakedb/snowflake"
      version = "~> 0.92.0"
    }
  }
}

provider "snowflake" {
  account = var.snowflake_account
  user = var.snowflake_username
  password = var.snowflake_password
  role = var.snowflake_role
  region = var.snowflake_region
  insecure_mode = true
}

variable "env" {
  type    = string
  default = "dev"  
}

# 1. Create database
resource "snowflake_database" "weather_db" {
  name = upper("WEATHER_${var.env}")
}

# 2. Create schemas
resource "snowflake_schema" "raw" {
  database = snowflake_database.weather_db.name
  name     = "RAW"
}

# 3. Create warehouse
resource "snowflake_warehouse" "weather_wh" {
  name           = upper("WEATHER_${var.env}_WH")
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

# 4. Create role
resource "snowflake_role" "weather_role" {
  name = upper("WEATHER_${var.env}_ROLE")
}

# 5. Grant database usage
resource "snowflake_database_grant" "weather_role_database_usage" {
  database_name = snowflake_database.weather_db.name
  privilege     = "USAGE"
  roles         = [snowflake_role.weather_role.name]
}

# 6. Grant warehouse usage
resource "snowflake_warehouse_grant" "weather_wh_usage" {
  warehouse_name = snowflake_warehouse.weather_wh.name
  privilege      = "USAGE"
  roles          = [snowflake_role.weather_role.name]
}

# 7. Grant role to user
resource "snowflake_role_grants" "weather_role_grant" {
  role_name = snowflake_role.weather_role.name
  users     = [var.snowflake_username] 
}

# 8. Grant schema usage
resource "snowflake_schema_grant" "weather_role_raw_all" {
  database_name = snowflake_database.weather_db.name
  schema_name   = snowflake_schema.raw.name
  privilege     = "ALL PRIVILEGES"
  roles         = [snowflake_role.weather_role.name]
}

# 9. Grant table privileges
resource "snowflake_table_grant" "weather_role_raw_all" {
  database_name = snowflake_database.weather_db.name
  schema_name   = snowflake_schema.raw.name
  privilege     = "ALL PRIVILEGES"
  on_all        = true
  roles         = [snowflake_role.weather_role.name]
}

# 10. Grant future table privileges
resource "snowflake_table_grant" "weather_role_raw_future_tables" {
  database_name = snowflake_database.weather_db.name
  schema_name   = snowflake_schema.raw.name
  privilege     = "ALL PRIVILEGES"
  on_future     = true
  roles         = [snowflake_role.weather_role.name]
}
