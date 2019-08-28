package sparkstarter.location

case class RawLocation(
                        id: String,
                        name: String,
                        address: String,
                        coordinates: RawCoordinates,
                        latitude: String,
                        longitude: String,
                        position: String
                      )
