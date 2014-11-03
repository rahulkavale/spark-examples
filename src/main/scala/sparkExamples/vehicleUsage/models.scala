package sparkExamples.vehicleUsage

import org.joda.time.DateTime

case class Vehicle(id: Int, name: String, state: String, servicinInterval: Int)

object Vehicle{
  def apply(input: String): Vehicle = {
    val separator = ","
    val splitInput = input.split(separator)
    val id = splitInput(0).toInt
    val name = splitInput(1)
    val state = splitInput(2)
    val servicingInterval = splitInput(3).toInt
    new Vehicle(id, name, state, servicingInterval)
  }
}
case class VehicleUsage(vehicleId: Int, usageRate: Int, curretUsage: Int, captureDate: DateTime)
object VehicleUsage{
  def apply(input: String): VehicleUsage = {
    val separator = ","
    val splitInput = input.split(separator)
    new VehicleUsage(splitInput(0).toInt, splitInput(1).toInt, splitInput(2).toInt, new DateTime(splitInput(3)))
  }
}


