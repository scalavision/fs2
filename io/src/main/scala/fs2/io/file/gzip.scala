package fs2
package io
package file

import java.nio.file.{Path, StandardOpenOption}
import java.time.{LocalDateTime, ZoneOffset}

import cats.effect.Sync

object gzip {

  //TODO: Skip the concept of returning a Pull[F, Byte, Unit]
  // instead return an Either of Header or something ???
  def headerValidator[F[_]](chunkSize: Int, offset: Long = 0L)(
      h: FileHandle[F]): Pull[F, Byte, Unit] = {

    val validHeader: Chunk.Bytes => Boolean = bytes =>
      bytes.at(0) == ID1 && bytes.at(1) == ID2 && bytes.at(2) == CM

    def skipOptionalField(bitIndex: Int): Byte => Int =
      byte =>
        if (byte.&(bitIndex) > 0) 1
        else 0

    def skipHeader(
        nrOfOptionalTextFields: Int,
        nrOfBytesInExtraFlags: Int,
        totalOffset: Long
    ): Pull[F, Byte, Unit] =
      // We first skip through optional flag extra
      if (nrOfBytesInExtraFlags > 0) {
        Pull.eval(h.read(nrOfBytesInExtraFlags, totalOffset)) >>
          Pull.outputChunk(Chunk.empty[Byte]) >> skipHeader(nrOfOptionalTextFields,
                                                            0,
                                                            totalOffset + nrOfBytesInExtraFlags)
        // Then we skip through optional textfields, searching for String terminator
      } else if (nrOfOptionalTextFields > 0) {
        Pull.eval(h.read(1, totalOffset)).flatMap {
          case Some(data) =>
            if (data(0) == 0x0)
              Pull.outputChunk(Chunk.empty[Byte]) >> skipHeader(nrOfOptionalTextFields - 1,
                                                                0,
                                                                totalOffset + 1)
            else {

              Pull.outputChunk(Chunk.empty[Byte]) >> skipHeader(nrOfOptionalTextFields,
                                                                0,
                                                                totalOffset + 1)
            }
          case None =>
            Pull.done
        }
        // Finally we finish up with an empty chunk
      } else
        Pull.outputChunk(Chunk.empty[Byte])

    def go(nextChunkSize: Int,
           totalOffset: Long,
           initialHeader: Boolean = true): Pull[F, Byte, Unit] =
      Pull.eval(h.read(nextChunkSize, totalOffset)).flatMap {

        case Some(data) =>
          val bytes: Chunk.Bytes = data.toBytes

          if (initialHeader) {
            if (bytes.length == initialHeaderLength && validHeader(bytes)) {

              val flags: Byte = bytes.at(4)
              val extraFlag = skipOptionalField(FEXTRA)(flags)
              //val hasCRC = skipOptionalField(FHCRC)(flags)

              //var innerOffset = 0

              val nrOfOptionalTextFields =
                skipOptionalField(FNAME)(flags) + skipOptionalField(FCOMMENT)(flags)

              println("textfields: " + nrOfOptionalTextFields)

              if (extraFlag > 0) {
                val extraBytes = bytes.at(9) + bytes.at(10)
                println(
                  "We have an extra flag of size: !" + extraBytes + " given byte the byte sequence: " + bytes
                    .at(9) + " " + bytes.at(10))
                skipHeader(nrOfOptionalTextFields, extraBytes, totalOffset)
              } else {
                skipHeader(nrOfOptionalTextFields, 0, totalOffset)
              }

            } else
              Pull.raiseError(
                new Throwable("Not a valid GZIP file, or compression method unsupported!"))
          } else {
            Pull.done
          }

        case None => Pull.done
      }

    go(initialHeaderLength, 0)

  }

  def unzip[F[_]: Sync](path: Path, chunkSize: Int): Stream[F, Byte] =
    pulls
      .fromPath(path, List(StandardOpenOption.READ))
      .flatMap { c =>
        headerValidator(chunkSize)(c.resource) //>>
      //pulls.readAllFromFileHandle(chunkSize)(c.resource)
      }
      .stream

  // gzip magic
  private final val ID1 = 0x1F.toByte
  private final val ID2 = 0x8b.toByte
  // gzip compression method, 8 is the only supported method
  private final val CM = 0x08.toByte

  // we take the number of bytes that gives the initial metainfo about the header
  private final val initialHeaderLength = 12

  // Bit offsets for the flag
  private final val FEXTRA = 4
  private final val FNAME = 8
  private final val FCOMMENT = 16
  private final val FHCRC = 2

  val errorMessage: (Int, Int, String) => String = (flag, value, order) =>
    s"GZIP Magic $order byte not $flag, but $value, this may not be a GZIP filetype"

  case class HeaderMetaData(
      id1: Byte,
      id2: Byte,
      cm: Byte,
      flags: Byte,
      mTime: (Byte, Byte, Byte, Byte),
      xfl: Byte,
      os: Byte,
      FExtraFlag: (Byte, Byte)
  ) {

    def osTable = Map(
      0 -> "FAT filesystem (MS-DOS, OS/2, NT/Win32)",
      1 -> "Amiga",
      2 -> "VMS (or OpenVMS)",
      3 -> "Unix",
      4 -> "VM/CMS",
      5 -> "Atari TOS",
      6 -> "HPFS filesystem (OS/2, NT)",
      7 -> "Macintosh",
      8 -> "Z-System",
      9 -> "CP/M",
      10 -> "TOPS-20",
      11 -> "NTFS filesystem (NT)",
      12 -> "QDOS",
      13 -> "Acorn RISCOS",
      255 -> "unknown"
    )

    def asHex: Byte => String = byte => String.format("%02X", new java.lang.Integer(byte & 0xff))

    def time: LocalDateTime = {
      val epoch = BigInt(Array[Byte](mTime._1, mTime._2, mTime._3, mTime._4)).longValue()
      LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC)
    }

    override def toString: String =
      s"""
         |id1:   ${asHex(id1)}
         |id2:   ${asHex(id2)}
         |cm:    ${asHex(cm)}
         |flags: ${flags.toBinaryString} (${asHex(flags)})
         |mTime: $time
         |xfl:   ${asHex(xfl)}
         |os:    ${asHex(os)} (${osTable.getOrElse(os, "Operating System id has wrong value: " + os)})
         |fExtraFlag: ${asHex(FExtraFlag._1) + asHex(FExtraFlag._2)}
       """.stripMargin
  }

  sealed trait GZipHeaderData
  case class ModificationTime(epoch: Long) extends GZipHeaderData
  case class ExtraField(value: Array[Byte]) extends GZipHeaderData
  case class FileName(value: String) extends GZipHeaderData
  case class Comment(value: String) extends GZipHeaderData
  case class CRC16(value: Option[(Byte, Byte)]) extends GZipHeaderData
  case class CRC32(value: (Byte, Byte, Byte, Byte)) extends GZipHeaderData
  case class ISIZE(value: (Byte, Byte, Byte, Byte)) extends GZipHeaderData

  case class GZipHeader(
      modificationTime: ModificationTime,
      extraField: Option[ExtraField],
      fileName: Option[FileName],
      comment: Option[Comment],
      CRC16: Option[CRC16],
      CRC32: CRC32,
      ISIZE: ISIZE,
      nrOfBytes: Long
  )

  final case class FileIsEmpty(data: Chunk[Byte]) extends Exception
  final case class InvalidGzipFile(data: Chunk[Byte]) extends Exception

  def extractHeaderMetaData[F[_]](
      h: FileHandle[F]): Pull[F, (FileHandle[F], HeaderMetaData), Unit] =
    Pull.eval(h.read(initialHeaderLength, 0)).flatMap {
      case Some(data) =>
        if (data.size < initialHeaderLength)
          Pull.raiseError(FileIsEmpty(data))
        else {
          Pull.output1[F, (FileHandle[F], HeaderMetaData)](
            (h,
             HeaderMetaData(
               id1 = data(0),
               id2 = data(1),
               cm = data(2),
               flags = data(3),
               mTime = (data(4), data(5), data(6), data(7)),
               xfl = data(8),
               os = data(9),
               FExtraFlag = (data(10), data(11))
             )))
        }

      case None =>
        Pull.done
    }

  sealed trait Steps
  case object ValidateHeaderFields extends Steps
  case object ExamineFlags extends Steps
  case class ExtractHeaderData(fExtra: Boolean, fName: Boolean, fComment: Boolean, fHRCR: Boolean)
      extends Steps

  def parseHeader[F[_]]: Pipe[F, (FileHandle[F], HeaderMetaData), Vector[GZipHeaderData]] = {

    def validateHeaderFields(
        headerMetaData: HeaderMetaData): Pull[F, Vector[GZipHeaderData], Unit] = {
      //TODO: output something sensible
      Pull.output1(???)
      ???
    }

    def examineFlags(headerMetaData: HeaderMetaData): Pull[F, Vector[GZipHeaderData], Unit] = {
      //TODO: output something sensible
      Pull.output1(???)
      ???
    }

    //TODO: iterate through the header data and return it with the offset
    def iterateHeaderBytes(headerMetaData: HeaderMetaData, steps: Steps)(
        h: FileHandle[F]): Pull[F, Vector[GZipHeaderData], Unit] = steps match {

      case ValidateHeaderFields =>
        validateHeaderFields(headerMetaData) >>
          iterateHeaderBytes(headerMetaData, ExamineFlags)(h)

      case ExamineFlags =>
        examineFlags(headerMetaData)

      case ExtractHeaderData(fExtra, fName, fComment, fHRCR) =>
        ???

    }

    def go(s: Stream[F, (FileHandle[F], HeaderMetaData)]): Pull[F, Vector[GZipHeaderData], Unit] =
      s.pull.unconsChunk.flatMap {
        case Some((dataChunk, tail)) =>
          val data = dataChunk.toList.headOption
          if (data.isDefined) {
            Pull.output1(Vector.empty[GZipHeaderData])
          } else Pull.raiseError(new Throwable("No header data available"))
        case None =>
          Pull.done
      }

    in =>
      go(in).stream
  }

  def gzipHeader[F[_]: Sync](path: Path): Stream[F, (FileHandle[F], HeaderMetaData)] =
    pulls
      .fromPath(path, List(StandardOpenOption.READ))
      .flatMap { c =>
        extractHeaderMetaData(c.resource)
      }
      .stream

}

/*

Old version kept for reference ...

  def extractHeaderInformation[F[_]](chunkSize: Int, offset: Long = 0L)(
      h: FileHandle[F]
  ): Pull[F, Byte, Unit] = {

    // skip through bytes that we don't need
    // return the new offset
    def skipBytes(size: Int, offset: Long, nrOfZeroTerminators: Int): Pull[F, Byte, Unit] =
      Pull.eval(h.read(size, offset)).flatMap {
        case Some(data) =>
          Pull.outputChunk(data)
        case None => Pull.done
      }

    // skip zero terminated strings in header
    // return new offset
//    def skipZeroTerminatedStrings(offset: Long): Long = ???

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
          // we count the number of flags with nullterminators
          // we extract the number of bytes we need to pull out
          if (extraBytesFlag > 0) {
            skipBytes(0, 0, 0)
            ???
          } else {
            Pull.outputChunk(initialHeader)
          }
        }

      case None => Pull.done

    }
  }

 */
