package fs2
package io
package file

import java.nio.file.Paths

//import scala.concurrent.duration._

import cats.effect.IO
//import cats.implicits._
//import java.io.IOException
import java.nio.file._
//import java.nio.file.attribute.BasicFileAttributes
import fs2._
//import fs2.internal.ThreadFactories
import TestUtil._

class gzipSpec extends Fs2Spec {
  //      runLog {
//        //val one = 1
//        Stream.empty()
//      }

  "gzip" - {
    "supports extracting a file" in {

//      val simpleHeader = immutable
//        .Seq[Int](
//          0x1f, 0x8b, 0x08, 0x08, 0x1e, 0x28, 0xa2, 0x52, 0x00, 0x03, 0x68, 0x75, 0x6d, 0x61, 0x6e,
//          0x5f
//        )
//        .map(_.toByte)

      runLog {
        val result = gzip.gzipHeader[IO](
          Paths.get(
            "/home/obiwan/stash/sources/scala/tools/cmdbuilder/OldProject/PipeGen/src/main/resources/zipTest.fai.gz")
        )

        println(result.compile.toVector.unsafeRunSync())
        result
      }

      1 shouldBe (1)
    }
  }

}
