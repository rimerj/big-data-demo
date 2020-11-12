package com.bhp.dp.utils

import java.math.BigInteger

import com.bhp.dp.testutils.TestSparkSession
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CryptoUtilsTest extends AnyFlatSpec with TestSparkSession {

  behavior of "CryptoUtils.fromHexString"
  it should "encrypt and decrypt" in {
    // Arrange
    val s="00FF"
    // Act
    val result=CryptoUtils.fromHexString(s)

    // Assert
    result.length shouldBe 2
    result(0) shouldBe 0
    result(1) shouldBe Integer.parseInt("FF", 16).toByte
  }

  behavior of "CryptoUtils.encrypt"
  it should "encrypt and decrypt" in {

    val pText = "SomePassword"
    // encrypt and decrypt need the same key.
    // get AES 256 bits (32 bytes) key
    val secretKey = CryptoUtils.getAESKeyFromPassword("test_password".toCharArray, "012345678901".getBytes)
    // encrypt and decrypt need the same IV.
    // AES-GCM needs IV 96-bit (12 bytes)
    val iv = "012345678901".getBytes
    val encryptedBytes = CryptoUtils.encrypt(pText.getBytes(CryptoUtils.UTF_8), secretKey, iv)

    CryptoUtils.toHexString(encryptedBytes) shouldBe "0B1C0227C7E9CB912D79BB9271ADE0EDD7823D989926590B167C7738"

    val decryptedBytes = CryptoUtils.decrypt(encryptedBytes, secretKey, iv)
    val resultString=new String(decryptedBytes, CryptoUtils.UTF_8)
    resultString shouldBe pText
    /*
    val OUTPUT_FORMAT = "%-30s:%s"
    System.out.println("\n------ AES GCM Encryption ------")
    System.out.println(String.format(OUTPUT_FORMAT, "Input (plain text)", pText))
    System.out.println(String.format(OUTPUT_FORMAT, "Key (hex)", CryptoUtils.toHexString(secretKey.getEncoded)))
    System.out.println(String.format(OUTPUT_FORMAT, "IV  (hex)", CryptoUtils.toHexString(iv)))
    System.out.println(String.format(OUTPUT_FORMAT, "Encrypted (hex) ", CryptoUtils.toHexString(encryptedBytes)))
    System.out.println(String.format(OUTPUT_FORMAT, "Encrypted (hex) (block = 16)", CryptoUtils.hexWithBlockSize(encryptedBytes, 16)))
    System.out.println("\n------ AES GCM Decryption ------")
    System.out.println(String.format(OUTPUT_FORMAT, "Input (hex)", CryptoUtils.toHexString(encryptedBytes)))
    System.out.println(String.format(OUTPUT_FORMAT, "Input (hex) (block = 16)", CryptoUtils.hexWithBlockSize(encryptedBytes, 16)))
    System.out.println(String.format(OUTPUT_FORMAT, "Key (hex)", CryptoUtils.toHexString(secretKey.getEncoded)))
    */

  }
}
