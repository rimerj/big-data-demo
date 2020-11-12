package com.bhp.dp.utils

import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec
import java.security.NoSuchAlgorithmException
import java.security.spec.InvalidKeySpecException
import java.util
import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import java.nio.charset.StandardCharsets


// from: https://mkyong.com/java/java-aes-encryption-and-decryption/
object CryptoUtils {
  val ENCRYPT_ALGO = "AES/GCM/NoPadding"
  val TAG_LENGTH_BIT = 128
  val IV_LENGTH_BYTE = 12
  val UTF_8 = StandardCharsets.UTF_8


  // Password derived AES 256 bits secret key
  @throws[NoSuchAlgorithmException]
  @throws[InvalidKeySpecException]
  def getAESKeyFromPassword(password: Array[Char], salt: Array[Byte]): SecretKeySpec = {
    val factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
    // iterationCount = 65536
    // keyLength = 256
    val spec = new PBEKeySpec(password, salt, 65536, 256)
    val secret = new SecretKeySpec(factory.generateSecret(spec).getEncoded, "AES")
    secret
  }

  // hex representation
  def toHexString(bytes: Array[Byte]):String = {
    val result = new StringBuilder
    for (b <- bytes) {
      result.append(String.format("%02x", Byte.box(b)))
    }
    result.toString.toUpperCase
  }

  def fromHexString(hexString:String):Array[Byte] = {
    val result = new Array[Byte](hexString.length >> 1)
    for (i <- 0 to result.length-1) {
      result(i) = Integer.parseInt(hexString.substring(i*2, i*2
        +2), 16).toByte
    }
    result
  }


  // print hex with block size split
  def hexWithBlockSize(bytes: Array[Byte], blockSizeArg: Int) = {
    val hex = toHexString(bytes)
    // one hex = 2 chars
    val blockSize = blockSizeArg * 2
    // better idea how to print this?
    val result = new util.ArrayList[String]
    var index = 0
    while (index < hex.length) {
      result.add(hex.substring(index, Math.min(index + blockSize, hex.length)))
      index += blockSize
    }
    result.toString
  }




  // AES-GCM needs GCMParameterSpec
  def encrypt(pText: Array[Byte], secret: SecretKey, iv: Array[Byte]):Array[Byte] = {
    val cipher = Cipher.getInstance(ENCRYPT_ALGO)
    cipher.init(Cipher.ENCRYPT_MODE, secret, new GCMParameterSpec(TAG_LENGTH_BIT, iv))
    val encryptedText = cipher.doFinal(pText)
    encryptedText
  }

  def decrypt(cText: Array[Byte], secret: SecretKey, iv: Array[Byte]):Array[Byte] = {
    val cipher = Cipher.getInstance(ENCRYPT_ALGO)
    cipher.init(Cipher.DECRYPT_MODE, secret, new GCMParameterSpec(TAG_LENGTH_BIT, iv))
    cipher.doFinal(cText)
  }

}