package com.bhp.dp.utils

import javax.crypto.spec.SecretKeySpec

/**
 * Common Arg Parsing stuff. Wish scallop worked... :(
 *
 * @param args
 */
abstract class JobArgs(args: Array[String]) {
  protected def getArgStringValue(argName: String, defaultVal: String): String = {
    val argIndex = args.indexOf(argName)

    if (argIndex == -1 || argIndex + 1 >= args.length) {
      // If arg wasn't found or value goes past arg array bounds use default
      defaultVal
    } else {
      val argValue = args(args.indexOf(argName) + 1)
      // Check in they provided the flag but didn't pass a value after it
      if (argValue.startsWith("-"))
        defaultVal
      else
        argValue
    }
  }

  protected def getArgBoolValue(argName: String, defaultVal: Boolean): Boolean = {
    getArgStringValue(argName, if (defaultVal) "1" else "0").trim == "1"
  }

  protected def getOptionalStringValue(argName: String): Option[String] = {
    val default = "$%&*(!#@$&*)$%&@*)$#%&" // would never be this value, would it?
    val derived = getArgStringValue(argName, default)
    if (default != derived) Some(derived) else None
  }

  protected def ensureTrailingSlash(param: String): String =
    if (param == "") "" // empty values aren't paths, so don't change...
    else if (!param.endsWith("/")) s"$param/" // populated strings need trail slash
    else param

  private val secretKey = CryptoUtils.getAESKeyFromPassword("BHP$DATA!PLATFORM$SAWE3OME!".toCharArray, "10987654321".getBytes)
  private val iv = "012345678901".getBytes

  protected def parseJdbcUrlKey(key: String, jdbcUrl: String): Option[String] = {
    val s=key+"="
    val startIndex=jdbcUrl.indexOf(s)
    if (startIndex == -1)
      return None
    val endIndex=jdbcUrl.indexOf(";", startIndex+s.length)
    if (endIndex == -1)
      return None
    Some(jdbcUrl.substring(startIndex+s.length, endIndex))
  }

  protected def decryptPassword(encryptedPasswordHexString: String, secretKey: SecretKeySpec = secretKey, iv: Array[Byte] = iv): String =
    new String(
      CryptoUtils.decrypt(CryptoUtils.fromHexString(encryptedPasswordHexString), secretKey, iv),
      CryptoUtils.UTF_8
    )
}