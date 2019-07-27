package sparkutils

import org.apache.log4j.{Level, Logger}

object LoggerUtil {
	def MyLogger={
		Logger.getLogger("org").setLevel(Level.WARN)
	}
}
