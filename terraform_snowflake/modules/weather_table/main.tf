resource "snowflake_table" "weather_data" {
  database = var.database_name
  schema   = var.schema_name
  name     = "WEATHER_DATA"

  column {
    name = "DATE"
    type = "DATE"
  }

  column {
    name = "LON"
    type = "FLOAT"
  }

  column {
    name = "LAT"
    type = "FLOAT"
  }

  column {
    name = "MAIN"
    type = "STRING"
  }

  column {
    name = "DESCRIPTION"
    type = "STRING"
  }

  column {
    name = "ICON"
    type = "STRING"
  }

  column {
    name = "TEMP"
    type = "FLOAT"
  }

  column {
    name = "FEELS_LIKE"
    type = "FLOAT"
  }

  column {
    name = "TEMP_MIN"
    type = "FLOAT"
  }

  column {
    name = "TEMP_MAX"
    type = "FLOAT"
  }

  column {
    name = "PRESSURE"
    type = "INT"
  }

  column {
    name = "HUMIDITY"
    type = "INT"
  }

  column {
    name = "WIND_SPEED"
    type = "FLOAT"
  }

  column {
    name = "WIND_DEG"
    type = "INT"
  }

  column {
    name = "CLOUDS"
    type = "INT"
  }

  column {
    name = "COUNTRY"
    type = "STRING"
  }

  column {
    name = "SUNRISE"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "SUNSET"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "DT"
    type = "TIMESTAMP_NTZ"
  }

  primary_key {
    keys = ["DATE", "LAT", "LON"]
  }
} 