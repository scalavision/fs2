package fs2
package io
package file

import java.nio.file.{Path, StandardOpenOption}

import cats.effect.{Sync}
import fs2.Stream

object gzip {

  // gzip magic
  private final val ID1 = 0x1F
  private final val ID2 = 0x8b
  private val CM = 8
  private val initialHeaderLength = 10

  val errorMessage: (Int, Int, String) => String = (flag, value, order) =>
    s"GZIP Magic $order byte not $flag, but $value, this may not be a GZIP filetype"

  def extractHeaderInformation[F[_]](chunkSize: Int, offset: Long = 0L)(
      h: FileHandle[F]): Pull[F, Byte, Unit] =
    Pull.eval(h.read(initialHeaderLength, offset)).flatMap {
      case Some(initialHeader) =>
        if (initialHeader(0) != ID1) {
          val error = errorMessage(initialHeader(0), ID1, "first")
          Pull.raiseError(new Throwable(error))
        } else if (initialHeader(1) != ID2) {
          val error = errorMessage(initialHeader(1), ID2, "second")
          Pull.raiseError(new Throwable(error))
        } else if (initialHeader(2) != CM) {
          Pull.raiseError(new Throwable("GZIP Compression method is not supported"))
        } else {
          //val flags = initialHeader(3)
          val extraBytesFlag = initialHeader(8)

          if (extraBytesFlag > 0) {
            ???
          } else {
            Pull.outputChunk(initialHeader)
          }
        }

      case None => Pull.done

    }

  def unzip[F[_]: Sync](path: Path, chunkSize: Int): Stream[F, Byte] =
    pulls
      .fromPath(path, List(StandardOpenOption.READ))
      .flatMap { c =>
        extractHeaderInformation(chunkSize)(c.resource) >>
          pulls.readAllFromFileHandle(chunkSize)(c.resource)
      }
      .stream

}
