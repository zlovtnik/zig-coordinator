package com.sslproxy.coordinator

import cats.effect.{IO, IOApp}

object Main extends IOApp.Simple:

  override def run: IO[Unit] =
    IO.println("zig-coordinator (Scala) — TiDB sink placeholder")