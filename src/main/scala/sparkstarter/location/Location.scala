package sparkstarter.location

case class Location(
                     id: Long,
                     name: String,
                     address: String,
                     coordinates: Coordinates,
                     position: String
                   )
