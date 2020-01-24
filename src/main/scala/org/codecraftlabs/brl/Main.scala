package org.codecraftlabs.brl

import org.apache.log4j.Logger

object Main {

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting")
  }
}
